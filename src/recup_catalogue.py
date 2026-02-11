import requests
import pandas as pd
from pathlib import Path
import time
import logging

DATA_DIR = Path("data")
CATALOG_DIR = DATA_DIR / "catalogue"
GTFS_DIR = DATA_DIR / "raw_gtfs"


def fetch_gtfs_datasets(page_size=1000, sleep=0.2):
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
        print(data[:1])
        if not data:
            break

        datasets.extend(data)

        if len(data) < page_size:
            break

        if page % 5 == 0:
            print(f"Page {page}, datasets récupérés : {len(datasets)}")

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

    return pd.DataFrame(rows)


if __name__ == "__main__":

    BASE_URL = "https://transport.data.gouv.fr/api/"

    logging.basicConfig(level=logging.INFO)

    # s'assurer que les dossiers existent
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    GTFS_DIR.mkdir(parents=True, exist_ok=True)

    print("Récupération des datasets GTFS depuis transport.data.gouv.fr...")
    datasets = fetch_gtfs_datasets()

    print("Récupération des datasets terminée, construction du catalogue...")
    catalogue = build_gtfs_catalog(datasets)
    print(f"Catalogue construit : {len(catalogue)} GTFS trouvés !")

    catalog_path = CATALOG_DIR / "gtfs_catalog.csv"
    catalogue.to_csv(catalog_path, index=False)
    print("Catalogue sauvegardé !")
