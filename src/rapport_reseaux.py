"""
Construction du rapport des réseaux (gtfs_datasets_info) à partir des données
brutes persistées par traitements_gtfs.py.

Attention, pour rapprocher un GTFS d'un agrégat, on se base sur le nom de l'agence.
Les agences trop génériques sont filtrées (ex: "Keolis", "Transdev", "SNCF"...)
Cette liste est donc vraisemblablement incomplète.
On aurait pu utiliser les métadonnées du PAN mais pas standardisées et dépendent de l'agrégat

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

# noms d'agence trop génériques pour identifier un réseau précis filtrés
AGENCES_GENERIQUES = {
    "keolis", "transdev", "ratp dev", "ratp développement",
    "sncf", "sncf voyageurs", "veolia transport", "vectalia",
    "tub", # saint brieuc mais aussi decazeville, bar-le-duc...,
    "distribus", # en bretagne et en alsace
}

def _agency_set(agency_name: str | None) -> set[str]:
    if not agency_name:
        return set()
    return {
        a.strip().lower() for a in agency_name.split(",")
        if a.strip() and a.strip().lower() not in AGENCES_GENERIQUES
    }


if "is_agregat" in gtfs_datasets_info.columns and "agency_name" in gtfs_datasets_info.columns:
    agregats_info = [
        {
            "resources_id": row["resources_id"],
            "page_url": row.get("page_url"),
            "agencies": _agency_set(row["agency_name"]),
            "geo_rank": GEO_RANK.get(row.get("categorie_geo"), 99),
        }
        for row in gtfs_datasets_info.filter(pl.col("is_agregat")).iter_rows(named=True)
        if _agency_set(row["agency_name"])
    ]
    log.info("%d agrégat(s) régional(aux) identifié(s) pour le rapprochement.", len(agregats_info))
    # log.info("Agences des agrégats : %s", [ag["agencies"] for ag in agregats_info])


    def _find_agregat(
            is_agregat: bool | None, agency_name: str | None, categorie_geo: str | None
    ) -> tuple[int | None, str | None]:
        """
        Retourne (resources_id, page_url) de l'agrégat correspondant à l'agence, ou (None, None).
        """
        if is_agregat:
            return (None, None)  # on ne rapproche pas un agrégat de lui-même

        agencies = _agency_set(agency_name)
        if not agencies:
            return (None, None)

        geo_rank = GEO_RANK.get(categorie_geo, 99)

        for ag in agregats_info:
            # le GTFS ne doit pas avoir une couverture géographique plus large que l'agrégat
            if geo_rank > ag["geo_rank"]:
                continue

            if agencies & ag["agencies"]:
                return (ag["resources_id"], ag["page_url"])

        return (None, None)


    matches = [
        _find_agregat(is_ag, ag_name, categorie_geo)
        for is_ag, ag_name, categorie_geo in zip(
            gtfs_datasets_info["is_agregat"].to_list(),
            gtfs_datasets_info["agency_name"].to_list(),
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
    log.warning("Colonnes is_agregat/agency_name absentes, rapprochement agrégat ignoré.")


# Sauvegarde
cols_a_suppr = ["slug"]
gtfs_datasets_info = gtfs_datasets_info.drop(cols_a_suppr, strict=False)

out_path = rapport_dir / f"{DATE_STR}_gtfs_datasets_info.parquet"
gtfs_datasets_info.write_parquet(out_path)
log.info("Rapport des réseaux écrit : %s (%d lignes).", out_path, len(gtfs_datasets_info))
log.info("Colonnes du rapport : %s", gtfs_datasets_info.columns)
