"""
Construction du rapport des réseaux (gtfs_datasets_info) à partir des données
brutes persistées par traitements_gtfs.py.

Pour rapprocher un GTFS individuel de l'agrégat régional qui le contient, on se base
sur les "offers" de l'API transport.data.gouv.fr (champ offer_ids, cf. utils_gtfs.
get_gtfs_datasets_info). Ces métadonnées sont en fait standardisées : chaque jeu de
données (agrégat ou GTFS individuel) porte la liste des offres de transport (réseaux)
qu'il couvre, identifiées par un identifiant_offre numérique stable. Un agrégat régional
a l'union des offres de tous ses réseaux membres ; un GTFS individuel a (en général) une
seule offre. Le rapprochement se fait donc par intersection des offer_ids, ce qui est
bien plus fiable que l'ancien matching sur agency_name (qui échouait notamment sur les
agences trop génériques comme "Keolis", "Transdev", "SNCF"...).

Nécessite que traitements_gtfs.py ait déjà tourné pour la date FRAICHEUR
(config.py), et donc que les fichiers suivants existent :
    - {date}_networks_raw.parquet
    - {date}_gtfs_datasets_raw.parquet
"""

import logging
import polars as pl
from config import FRAICHEUR, DATA_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATE_STR = str(FRAICHEUR).replace("-", "")
rapport_dir = DATA_DIR / "transport.data.gouv.fr"

networks_path = rapport_dir / f"{DATE_STR}_networks_raw.parquet"
gtfs_datasets_path = rapport_dir / f"{DATE_STR}_gtfs_datasets_raw.parquet"

if not networks_path.exists():
    raise FileNotFoundError(
        f"{networks_path} introuvable : lancez d'abord traitements_gtfs.py pour {FRAICHEUR}."
    )

networks_df = pl.read_parquet(networks_path)
log.info("networks_df chargé (%d lignes).", len(networks_df))

if gtfs_datasets_path.exists():
    gtfs_datasets = pl.read_parquet(gtfs_datasets_path)
    log.info("gtfs_datasets chargé (%d lignes).", len(gtfs_datasets))
else:
    log.warning("%s introuvable, rapport limité aux données observées.", gtfs_datasets_path)
    gtfs_datasets = pl.DataFrame()

if len(gtfs_datasets) > 0 and "resources_id" in gtfs_datasets.columns:
    gtfs_datasets_info = gtfs_datasets.join(networks_df, on="resources_id", how="left")
else:
    gtfs_datasets_info = networks_df


# Rapprochement GTFS individuel <-> agrégat régional

# Filtre géographique, un agrégat est plus large qu'un GTFS individuel
GEO_RANK = {"commune": 0, "epci": 1, "departement": 2, "region": 3, "pays": 4}


def _offer_id_set(offer_ids: list[int] | None) -> set[int]:
    return set(offer_ids) if offer_ids else set()


if "is_agregat" in gtfs_datasets_info.columns and "offer_ids" in gtfs_datasets_info.columns:
    agregats_info = [
        {
            "resources_id": row["resources_id"],
            "page_url": row.get("page_url"),
            "offer_ids": _offer_id_set(row["offer_ids"]),
            "geo_rank": GEO_RANK.get(row.get("categorie_geo"), 99),
        }
        for row in gtfs_datasets_info.filter(pl.col("is_agregat")).iter_rows(named=True)
        if _offer_id_set(row["offer_ids"])
    ]
    log.info(
        "%d agrégat(s) régional(aux) identifié(s) pour le rapprochement (via offers API).",
        len(agregats_info),
    )
    # log.info("Offer ids des agrégats : %s", [ag["offer_ids"] for ag in agregats_info])


    def _find_agregat(
            is_agregat: bool | None, offer_ids: list[int] | None, categorie_geo: str | None
    ) -> tuple[int | None, str | None]:
        """
        Retourne (resources_id, page_url) de l'agrégat correspondant à l'offre, ou (None, None).
        """
        if is_agregat:
            return (None, None)  # on ne rapproche pas un agrégat de lui-même

        offers = _offer_id_set(offer_ids)
        if not offers:
            return (None, None)

        geo_rank = GEO_RANK.get(categorie_geo, 99)

        for ag in agregats_info:
            # le GTFS ne doit pas avoir une couverture géographique plus large que l'agrégat
            if geo_rank > ag["geo_rank"]:
                continue

            if offers & ag["offer_ids"]:
                return (ag["resources_id"], ag["page_url"])

        return (None, None)


    matches = [
        _find_agregat(is_ag, offer_ids, categorie_geo)
        for is_ag, offer_ids, categorie_geo in zip(
            gtfs_datasets_info["is_agregat"].to_list(),
            gtfs_datasets_info["offer_ids"].to_list(),
            gtfs_datasets_info["categorie_geo"].to_list(),
        )
    ]
    gtfs_datasets_info = gtfs_datasets_info.with_columns([
        pl.Series("agregat_resources_id", [m[0] for m in matches]),
        pl.Series("agregat_url", [m[1] for m in matches]),
    ])
    nb_matches = sum(1 for m in matches if m[0] is not None)
    log.info("%d GTFS individuel(s) rapproché(s) d'un agrégat.", nb_matches)
else:
    log.warning("Colonnes is_agregat/offer_ids absentes, rapprochement agrégat ignoré.")


# Sauvegarde
cols_a_suppr = ["slug"]
gtfs_datasets_info = gtfs_datasets_info.drop(cols_a_suppr, strict=False)

# Conversion des colonnes nested pour l'export CSV
for col, dtype in zip(gtfs_datasets_info.columns, gtfs_datasets_info.dtypes):
    if isinstance(dtype, pl.List):
        inner_dtype = dtype.inner

        if isinstance(inner_dtype, pl.Struct):
            # Liste de structures -> JSON
            gtfs_datasets_info = gtfs_datasets_info.with_columns(
                pl.col(col)
                .list.eval(pl.element().struct.json_encode())
                .list.join(",")
                .alias(col)
            )
        else:
            # Liste de valeurs -> conversion en str puis concaténation
            gtfs_datasets_info = gtfs_datasets_info.with_columns(
                pl.col(col)
                .list.eval(pl.element().cast(pl.String))
                .list.join(",")
                .alias(col)
            )

    elif isinstance(dtype, pl.Struct):
        # Structure simple -> JSON
        gtfs_datasets_info = gtfs_datasets_info.with_columns(
            pl.col(col).struct.json_encode().alias(col)
        )

out_path = rapport_dir / f"{DATE_STR}_gtfs_datasets_info.csv"
gtfs_datasets_info.write_csv(out_path)

log.info(
    "Rapport des réseaux écrit : %s (%d lignes).",
    out_path,
    len(gtfs_datasets_info),
)
log.info("Colonnes du rapport : %s", gtfs_datasets_info.columns)
