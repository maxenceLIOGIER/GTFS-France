import zipfile
from pathlib import Path
import pandas as pd
import unicodedata
import re


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
    name = re.sub(r"\.zip$", "", name, flags=re.IGNORECASE)

    match = re.search(r"interurbains_des_(.+)$", name)
    if not match:
        match = re.search(r"de_(.+)$", name)
        if not match:
            match = re.search(r"du_(.+)$", name)
            if not match:
                match = re.search(r"d'(.+)$", name)
                if not match:
                    return None
    region = match.group(1)

    # Nettoyage
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


STOPWORDS = {
    "reseau",
    "transport",
    "transports",
    "mobilite",
    "mobilites",
    "mobilité",
    "mobilités",
    "agglo",
    "agglomeration",
    "communaute",
    "communauté",
    "metropole",
    "métropole",
    "syndicat",
    "regie",
    "régie",
    "aom",
}


def extract_keywords(text):
    """
    Détermine si une AOM est présente dans un agrégat.
    On se base sur les mots-clés présents dans le nom de l'agrégat
    En filtrant les stopwords et les mots trop petits
    """

    text = text.lower()
    text = text.strip()
    parts = text.split(" ")

    keywords = []
    for p in parts:

        # nettoyage accents
        p = p.strip()
        p = unicodedata.normalize("NFD", p)
        p = "".join(c for c in p if unicodedata.category(c) != "Mn")

        # filtre stopwords et mots trops petits
        if p not in STOPWORDS and len(p) > 2:
            keywords.append(p)

    return list(set(keywords))


def missing_aom_in_agregat(agregat_zip, catalogue_aom):
    """
    Pour chaque agrégat, on regarde quelles AOMS manquent à l'appel.
    Pour cela, on compare les mots-clés

    TODO: il faudrait une approche plus robuste à l'avenir
    """
    with zipfile.ZipFile(agregat_zip, "r") as z:
        agency = pd.read_csv(z.open("agency.txt"))
        agency_agregate_names = agency["agency_name"].str.lower().tolist()

    code_region = extract_region(agregat_zip.name)
    catalogue_aom_region = catalogue_aom[
        catalogue_aom["Code INSEE Region"] == code_region
    ]
    print(f"Région extraite: {code_region}, nombre d'AOM : {len(catalogue_aom_region)}")
    aom_names_catalogue = catalogue_aom_region["Nom de l’AOM"].str.lower().tolist()

    missing_aoms = pd.DataFrame(columns=["aom_manquante", "région", "agregat"])
    for aom_name in aom_names_catalogue:
        keywords = set(extract_keywords(aom_name))
        found = any(
            keywords & set(extract_keywords(agency_name))
            for agency_name in agency_agregate_names
        )
        if not found:
            new_row = {
                "aom_manquante": aom_name,
                "région": code_region,
                "agregat": agregat_zip.name,
            }
            missing_aoms = pd.concat(
                [missing_aoms, pd.DataFrame([new_row])], ignore_index=True
            )

    return missing_aoms


if __name__ == "__main__":
    catalogue_aom = pd.read_csv("data/catalogues/aom_catalog.csv")
    liste_agregats_zip = listing_agregats_zip()

    all_missing_aoms = pd.DataFrame(columns=["aom_manquante", "région", "agregat"])
    for agregat_zip in liste_agregats_zip:
        print(f"Inspection de {agregat_zip.name}:")
        missing_aom = missing_aom_in_agregat(agregat_zip, catalogue_aom)
        if not missing_aom.empty:
            all_missing_aoms = pd.concat(
                [all_missing_aoms, missing_aom], ignore_index=True
            )

    nb_missing = len(all_missing_aoms)
    print(f"\nNombre d'AOM manquantes au total: {nb_missing}")
    print("Résumé des AOM manquantes par agrégat:")
    for agregat_zip, missing_aoms in all_missing_aoms.groupby("agregat"):
        print(f"{agregat_zip}: {len(missing_aoms)} AOM manquantes")

    # On enregistre les résultats
    all_missing_aoms.to_csv("data/catalogues/missing_aoms.csv", index=False)
