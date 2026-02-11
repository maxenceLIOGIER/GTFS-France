"""
Script n°2 :
- Télécharge les agrégats GTFS à partir du catalogue.
- Sauvegarde les fichiers ZIP
"""

import requests
from pathlib import Path
import pandas as pd


def fetch_agregates(catalogue):
    agregates = catalogue[
        catalogue["dataset_title"].str.contains("agrégat", case=False, na=False)
        | catalogue["dataset_title"].str.contains("agregat", case=False, na=False)
    ]
    return agregates


def download_file(agregat):
    url = agregat["resource_url"]
    dataset_name = agregat["dataset_title"].replace(" ", "_").lower()
    output_path = Path("data/raw_gtfs/agregats") / f"{dataset_name}.zip"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()

        with open(output_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)

    print(f"Téléchargé : {output_path}")


if __name__ == "__main__":

    catalogue_gtfs = pd.read_csv("data/catalogues/gtfs_catalog.csv")
    print(f"{len(catalogue_gtfs)} GTFS trouvés dans le catalogue")

    agregates = fetch_agregates(catalogue_gtfs)
    print(f"{len(agregates)} agrégats")

    for _, row in agregates.iterrows():
        download_file(row)
