import zipfile
from pathlib import Path
import pandas as pd


def listing_agregats_zip():
    zip_dir = Path("data/raw_gtfs/agregats")
    zip_files = list(zip_dir.glob("*.zip"))
    return zip_files


def inspect_gtfs(agregat_zip):
    """
    Pour chaque agrégat, on regarde quelles AOMS sont présentes
    Puis on vérifie que cela corresponde au catalogue des AOM, avec un filtre par région
    """
    with zipfile.ZipFile(agregat_zip, "r") as z:
        agency = pd.read_csv(z.open("agency.txt"))
        agency_names = agency["agency_name"].str.lower().tolist()

    missing_aoms = []
    # TODO : Il y a un problème de clé de jointure entre les AOM et les GTFS
    # Absence de code INSEE dans les GTFS
    # il faudra probablement passer par une jointure spatiale, si possible côté aom_catalog

    # TODO : réussir à associer à chaque GTFS un code région pour le filtre

    return missing_aoms


if __name__ == "__main__":

    liste_aoms = pd.read_csv("data/catalogues/aom_catalog.csv")
    print(f"{len(liste_aoms)} AOM dans le catalogue.")

    liste_agregats_zip = listing_agregats_zip()
    print(f"{len(liste_agregats_zip)} agrégats GTFS trouvés.")

    all_missing_aoms = {}
    for agregat_zip in liste_agregats_zip:
        print(f"Inspection de {agregat_zip}:")
        missing_aoms_agregat = inspect_gtfs(agregat_zip)
        if missing_aoms_agregat:
            all_missing_aoms[agregat_zip] = missing_aoms_agregat

    print("\nRésumé des AOM manquantes par agrégat:")
    for agregat_zip, missing_aoms in all_missing_aoms.items():
        print(f"{agregat_zip}: {len(missing_aoms)} AOM manquantes")
        if missing_aoms:
            print(f"  AOM manquantes: {', '.join(missing_aoms)}")
        else:
            print("  Toutes les AOM sont présentes dans cet agrégat.")
