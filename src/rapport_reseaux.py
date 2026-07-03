"""
Construction du rapport des réseaux (gtfs_datasets_info) à partir des données
brutes persistées par traitements_gtfs.py.

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
    gtfs_datasets_info_out = gtfs_datasets.join(networks_df, on="resources_id", how="left")
else:
    gtfs_datasets_info_out = networks_df

cols_a_suppr = ["slug"]
gtfs_datasets_info_out = gtfs_datasets_info_out.drop(cols_a_suppr, strict=False)

out_path = rapport_dir / f"{DATE_STR}_gtfs_datasets_info.parquet"
gtfs_datasets_info_out.write_parquet(out_path)
log.info("Rapport des réseaux écrit : %s (%d lignes).", out_path, len(gtfs_datasets_info_out))
log.info("Colonnes du rapport : %s", gtfs_datasets_info_out.columns)
