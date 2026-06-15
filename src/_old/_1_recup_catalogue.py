"""
Script n°1 :
- Récupère les datasets GTFS depuis l'API de transport.data.gouv.fr
- Construit un catalogue recensant les métadonnées pour chaque GTFS
- Sauvegarde le catalogue au format CSV
"""

from pathlib import Path
import time
import logging
import requests
import pandas as pd

DATA_DIR = Path("data")
CATALOG_DIR = DATA_DIR / "catalogues"
BASE_URL = "https://transport.data.gouv.fr/api"


def fetch_gtfs_datasets(page_size=1000, sleep=0.5):
    datasets = []
    page = 1

    while True:
        params = {"q": "gtfs", "sort": "-updated", "page": page, "page_size": page_size}

        r = requests.get(f"{BASE_URL}/datasets/", params=params, timeout=(3, 10))
        if r.status_code != 200:
            print(f"⚠️  Page {page} – status {r.status_code}, on saute")
            page += 1
            continue

        data = r.json()
        if not data:
            break

        datasets.extend(data)

        if len(data) < page_size:
            break

        page += 1
        time.sleep(sleep)

    return datasets


def build_gtfs_catalog(datasets):
    rows = []

    for ds in datasets:
        for res in ds.get("resources", []):

            if res.get("format", "").upper() != "GTFS":
                continue

            rows.append(
                {
                    "dataset_id": ds["id"],
                    "dataset_title": ds.get("title"),
                    "dataset_type": ds.get("type"),
                    "publisher": ds.get("publisher", {}).get("name"),
                    "covered_area": ds.get("covered_area"),
                    "dataset_updated": ds.get("updated"),
                    "resource_id": res.get("id"),
                    "resource_title": res.get("title"),
                    "resource_url": res.get("url"),
                    "resource_updated": res.get("updated"),
                }
            )

    catalogue_gtfs = pd.DataFrame(rows)

    print(f"Catalogue construit : {len(catalogue_gtfs)} GTFS trouvés !")

    gtfs_catalog_path = CATALOG_DIR / "gtfs_catalog.csv"
    catalogue_gtfs.to_csv(gtfs_catalog_path, index=False)
    print("Catalogue GTFS sauvegardé !")


def catalogue_aoms():
    url = (
        "https://www.data.gouv.fr/api/1/datasets/r/ef24f052-1eb9-4e2b-870f-e9c6024c83d2"
    )
    # cet url télécharge directement un fichier ods, trouvé depuis cette page :
    # www.data.gouv.fr/datasets/liste-et-composition-des-autorites-organisatrices-de-la-mobilite-aom

    aoms = pd.read_excel(url, engine="odf")
    print(f"Catalogue construit : {aoms.shape[0]} AOM trouvées !")

    aom_catalog_path = CATALOG_DIR / "aom_catalog.csv"
    aoms.to_csv(aom_catalog_path, index=False)
    print("Catalogue AOM sauvegardé !")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    # s'assurer que les dossiers existent
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)

    print("Récupération des datasets GTFS depuis transport.data.gouv.fr...")
    datasets = fetch_gtfs_datasets()
    build_gtfs_catalog(datasets)

    print("\nRécupération des AOM...")
    catalogue_aoms()
