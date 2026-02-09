import requests
import pandas as pd
from pathlib import Path
from tqdm import tqdm
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

DATA_DIR = Path("data")
CATALOG_DIR = DATA_DIR / "catalogue"
GTFS_DIR = DATA_DIR / "raw_gtfs"


from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def build_session():
    retry = Retry(
        total=4,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry)

    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({"User-Agent": "gtfs-france-collector/0.1"})

    return session


def fetch_gtfs_ressources(page_size=100, sleep=0.2):

    ressources = []
    page = 1
    session = build_session()

    while True:
        params = {"format": "GTFS", "page": page, "page_size": page_size}

        print(f"→ Resources page {page}")
        r = session.get(f"{BASE_URL}/resources/", params=params, timeout=(3, 10))

        if r.status_code != 200:
            print(f"⚠️  Page {page} – status {r.status_code}, on saute")
            page += 1
            continue

        data = r.json()
        page_data = data.get("data", [])

        if not page_data:
            break

        ressources.extend(page_data)

        meta = data.get("meta", {}).get("pagination", {})
        if page >= meta.get("pages", page):
            break

        if page % 5 == 0:
            print(f"Page {page}, Resources récupérés : {len(ressources)}")

        page += 1
        time.sleep(sleep)

    return ressources


def build_catalog_from_resources(resources):
    rows = []

    for r in resources:
        ds = r.get("dataset", {})

        rows.append(
            {
                "resource_id": r["id"],
                "resource_title": r.get("title"),
                "resource_url": r.get("url"),
                "resource_last_modified": r.get("last_modified"),
                "dataset_id": ds.get("id"),
                "dataset_title": ds.get("title"),
                "organization": ds.get("organization", {}).get("name"),
                "organization_id": ds.get("organization", {}).get("id"),
                "region": ds.get("spatial", {}).get("region"),
                "covered_area": ds.get("covered_area"),
            }
        )

    return pd.DataFrame(rows)


if __name__ == "__main__":

    BASE_URL = "https://transport.data.gouv.fr/api/1"

    logging.basicConfig(level=logging.INFO)

    # # ensure data directories exist
    # DATA_DIR.mkdir(parents=True, exist_ok=True)
    # CATALOG_DIR.mkdir(parents=True, exist_ok=True)
    # GTFS_DIR.mkdir(parents=True, exist_ok=True)

    print("Récupération des datasets GTFS depuis transport.data.gouv.fr...")
    ressources = fetch_gtfs_ressources(page_size=50, sleep=0.5)

    print("Récupération des datasets terminée, construction du catalogue...")
    catalogue = build_catalog_from_resources(ressources)
    print("Catalogue construit !")

    catalog_path = CATALOG_DIR / "gtfs_catalog.csv"
    catalogue.to_csv(catalog_path, index=False)

    print(f"{len(catalogue)} ressources GTFS trouvées")
    print(f"Catalogue sauvegardé : {catalog_path}")
