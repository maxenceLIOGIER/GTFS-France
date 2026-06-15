from pathlib import Path
import unicodedata
import re
import requests
import pandas as pd

from _old._2_dl_agregats import fetch_agregates


def listing_agregats_zip():
    """
    Il y a plusieurs agrégats en format GTFS en format zip.
    Cette fonction les liste.
    """
    zip_dir = Path("data/raw_gtfs/agregats")
    zip_files = list(zip_dir.glob("*.zip"))
    return zip_files


def extract_region(name):
    """
    Extrait la région d'un nom d'agrégat.
    On se base sur les préfixes "d'", "de_", "du_" et "des_".
    """
    # Extraction
    match = re.search(r"interurbains des (.+)$", name)
    if not match:
        match = re.search(r"de (.+)$", name)
        if not match:
            match = re.search(r"du (.+)$", name)
            if not match:
                match = re.search(r"d'(.+)$", name)
                if not match:
                    return None
    region = match.group(1)

    # Nettoyage
    region = region.lower()
    region = region.translate(str.maketrans({"-": " ", "_": " "}))
    region = unicodedata.normalize("NFD", region)
    region = "".join(c for c in region if unicodedata.category(c) != "Mn")

    # Code région
    dict_reg_code = {
        # "ile de france": "11",
        "centre val de loire": "24",
        # "bourgogne franche comte": "27",
        "normandie": "28",
        # "hauts de france": "32",
        "grand est": "44",
        "pays de la loire": "52",
        "bretagne": "53",
        "nouvelle aquitaine": "75",
        # "occitanie": "76",
        "auvergne rhone alpes": "84",
        # "provence alpes côte d'azur": "93",
    }
    # Certaines régions commentées car n'ont pas d'agrégats
    # A décommenter au besoin, juste une question de clarté pour l'instant

    code_region = dict_reg_code.get(region, None)
    return code_region


def missing_aom_in_agregat(agregat, catalogue_aom, url_template):
    """
    Pour chaque agrégat, on regarde quelles AOMS manquent à l'appel.
    Pour cela, on compare les codes siren.
    """

    # Requête API transport.data.gouv.fr pour récupérer les aoms dans l'agrégat
    dataset_id = agregat["dataset_id"]
    response = requests.get(url_template.format(dataset_id=dataset_id), timeout=30)
    data = response.json()
    legal_owners = data.get("legal_owners", [])

    # On récupère les siren des aoms dans l'agrégat
    sirens_in_agregat = set(
        {owner["siren"] for owner in legal_owners if owner["type"] == "aom"}
    )

    # Code région de l'agrégat pour filtrer les AOM manquantes par région
    code_region = extract_region(agregat["dataset_title"])
    cat_aom_region = catalogue_aom[catalogue_aom["Code INSEE Region"] == code_region]
    cat_aom_region["N° SIRENAOM"] = cat_aom_region["N° SIRENAOM"].astype(str)
    sirens_in_catalogue = set(cat_aom_region["N° SIRENAOM"])

    # AOM manquantes dans l'agrégat
    missing_sirens = sirens_in_catalogue - sirens_in_agregat
    liste_missing_aoms = cat_aom_region[
        cat_aom_region["N° SIRENAOM"].isin(missing_sirens)
    ]

    # Attention, dans le catalogue des AOM, il y a des siren en doublons
    # Ce sont les aom du type Région Auvergne-Rhône-Alpes (CC Auzon Communauté)

    siren_to_filter = [
        "200053767",
        "200053726",
        "233500016",
        "234500023",
        "200076958",
        "200052264",
        "239730013",
        "200053742",
        "229850003",
        "200053403",
        "200053759",
        "200053791",
        "234400034",
        "231300021",
    ]

    liste_missing_aoms = liste_missing_aoms[
        ~liste_missing_aoms["N° SIRENAOM"].isin(siren_to_filter)
    ]

    # Mise en forme des résultats
    missing_aoms = pd.DataFrame(
        {
            "aom_manquante": liste_missing_aoms["Nom de l’AOM"],
            "siren": liste_missing_aoms["N° SIRENAOM"],
            "région": liste_missing_aoms["Région"],
            "agregat": agregat["dataset_title"],
        }
    )

    return missing_aoms


if __name__ == "__main__":
    catalogue_aom = pd.read_csv("data/catalogues/aom_catalog.csv")
    catalogue_gtfs = pd.read_csv("data/catalogues/gtfs_catalog.csv")
    liste_agregats = fetch_agregates(catalogue_gtfs)

    URL_TEMPLATE = "https://transport.data.gouv.fr/api/datasets/{dataset_id}"

    all_missing_aoms = pd.DataFrame(
        columns=["aom_manquante", "siren", "région", "agregat"]
    )
    print("Inspection des agrégats...")
    for _, agregat in liste_agregats.iterrows():
        missing_aom = missing_aom_in_agregat(agregat, catalogue_aom, URL_TEMPLATE)
        if not missing_aom.empty:
            all_missing_aoms = pd.concat(
                [all_missing_aoms, missing_aom], ignore_index=True
            )

    nb_missing = len(all_missing_aoms)
    print(f"\nNombre d'AOM manquantes au total: {nb_missing}")
    print("Résumé des AOM manquantes par agrégat:")
    for region, missing_aoms in all_missing_aoms.groupby("région"):
        print(f"{region}: {len(missing_aoms)} AOM manquantes")

    # On enregistre les résultats
    all_missing_aoms.to_csv("data/catalogues/missing_aoms.csv", index=False)
