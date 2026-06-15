"""
Agrégation de tous les GTFS disponibles sur transport.data.gouv.fr
Conversion Python/Polars du script R original
"""

import os
import re
import gc
import zipfile
import unicodedata
import logging
import socket
import traceback
from pathlib import Path
from datetime import date
from io import StringIO

import polars as pl
import requests

# ─────────────────────────────────────────────
# Configuration du logger
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Paramètres
# ─────────────────────────────────────────────

# FRAICHEUR = date.today()
FRAICHEUR = date(2026, 6, 1)
FRAICHEUR_JOUR = FRAICHEUR.strftime("%A").lower()  # ex: "monday"
REDOWNLOAD = False

DATA_DIR = Path(os.environ.get("DATA_DIR", Path.cwd() / "data"))
BASE_DIR = DATA_DIR / "transport.data.gouv.fr" / str(FRAICHEUR)
RAW_DIR = DATA_DIR / "transport.data.gouv.fr" / "raw_tables" / str(FRAICHEUR)
CONSOLIDATED_DIR = DATA_DIR / "transport.data.gouv.fr" / "consolidated" / str(FRAICHEUR)

for d in [BASE_DIR, RAW_DIR, CONSOLIDATED_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# Mots-clés stop par type de transport
ROUTE_TYPE_MAP = {
    **{str(k): "bus" for k in [3, 11, *range(200, 210), *range(700, 717), 800]},
    **{str(k): "tramway" for k in [0, 5, *range(900, 907)]},
    **{str(k): "métro" for k in [1, *range(400, 406)]},
    **{str(k): "train" for k in [2, *range(100, 118)]},
    **{str(k): "autres" for k in [
        4, 6, 7, 12, 1000, 1100, 1200, *range(1300, 1308),
        1400, *range(1500, 1508), *range(1700, 1703)
    ]},
}
ROUTE_TYPE_PRIORITY = {"train": 1, "métro": 2, "tramway": 3, "bus": 4, "bus TAD": 4}

STOPWORDS_FR = {
    "le", "la", "les", "de", "du", "des", "un", "une", "et", "en", "au",
    "aux", "à", "par", "sur", "sous", "pour", "dans", "ce", "se", "sa",
    "son", "ses", "que", "qui", "ne", "pas", "plus", "ou", "y", "il",
    "elle", "ils", "elles", "je", "tu", "nous", "vous", "mon", "ton",
    "rer", "terminus", "hall", "depart", "arrivee",
    # "est" intentionnellement exclu
}

ABBREV_MAP = {
    "saint": "st", "sainte": "ste", "route": "rte",
    "chemin": "ch", "boulevard": "bd", "avenue": "av",
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def slugify(text: str) -> str:
    """Translitère, met en minuscule, supprime ponctuation/chiffres."""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    text = text.lower()
    text = re.sub(r"[\d\W_]+", " ", text)
    return text.strip()


def reduce_stop_name(name: str) -> str:
    """Réduit un nom d'arrêt en supprimant les stopwords et en remplaçant les abréviations."""
    tokens = slugify(name).split()
    tokens = [ABBREV_MAP.get(t, t) for t in tokens if t not in STOPWORDS_FR and len(t) > 1]
    return " ".join(tokens) if tokens else name.lower()


# def parse_gtfs_time(t: str) -> int | None:
#     """Retourne les secondes depuis minuit (gère > 24h)."""
#     try:
#         h, m, s = map(int, t.split(":"))
#         return h * 3600 + m * 60 + s
#     except Exception:
#         return None


def extract_gtfs_zip(gtfs_zip_file: Path, files: list[str] | None = None) -> dict:
    """Extrait les fichiers .txt d'un zip GTFS dans son dossier parent.

    Réplique territoRy::extract_gtfs_zip() :
    - Liste les fichiers du zip
    - Correspond les noms demandés via pmatch (basename)
    - Extrait avec junkpaths=True (flatten des sous-dossiers)
    - Retourne {"files": "a.txt, b.txt, ...", "extracted": bool}
    """
    destdir = gtfs_zip_file.parent

    # Liste les fichiers dans l'archive
    try:
        with zipfile.ZipFile(gtfs_zip_file) as zf:
            archive_names = zf.namelist()
    except Exception as e:
        log.debug("Impossible d'ouvrir %s : %s", gtfs_zip_file, e)
        return {"files": None, "extracted": False}

    # pmatch : pour chaque nom demandé, trouve le fichier dans l'archive par basename
    if files is not None:
        matched = []
        for wanted in files:
            wanted_base = Path(wanted).name
            candidates = [n for n in archive_names if Path(n).name == wanted_base]
            if candidates:
                matched.append(candidates[0])
        files_to_extract = matched if matched else []
    else:
        files_to_extract = archive_names

    if not files_to_extract:
        return {"files": "", "extracted": False}

    # Extraction avec flatten (junkpaths) : on pose tous les .txt à la racine de destdir
    extracted_names = []
    try:
        with zipfile.ZipFile(gtfs_zip_file) as zf:
            for member in files_to_extract:
                basename = Path(member).name
                dest_path = destdir / basename
                with zf.open(member) as src, open(dest_path, "wb") as dst:
                    dst.write(src.read())
                extracted_names.append(basename)
    except Exception as e:
        log.debug("Erreur d'extraction %s : %s", gtfs_zip_file, e)
        return {"files": "", "extracted": False}

    extraction_success = len(extracted_names) > 0
    return {
        "files": ", ".join(extracted_names),
        "extracted": extraction_success,
    }


def _clean_column_name(name: str) -> str:
    """Équivalent de janitor::clean_names() : snake_case, sans caractères spéciaux."""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(c for c in name if not unicodedata.combining(c))
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return name or "col"


def read_gtfs_dataset(
        gtfs_dir: Path, files_to_keep: list[str] | None = None, **kwargs
    ) -> dict[str, pl.DataFrame]:
    """Lit tous les fichiers .txt d'un dossier GTFS en DataFrames Polars.

    Réplique territoRy::read_gtfs_dataset() :
    - Lit tous les *.txt du dossier (ou seulement files_to_keep)
    - Toutes les colonnes en str (colClasses = "character")
    - Noms de colonnes nettoyés (janitor::clean_names → snake_case)
    - Supprime les colonnes sans nom (commençant par "...")
    - Supprime les guillemets résiduels (str_remove_all(x, '"'))
    """
    gtfs_files = sorted(gtfs_dir.glob("*.txt"))

    if files_to_keep is not None:
        gtfs_files = [f for f in gtfs_files if f.stem in files_to_keep]

    result: dict[str, pl.DataFrame] = {}
    for f in gtfs_files:
        table_name = f.stem
        try:
            raw = f.read_bytes().decode("utf-8-sig", errors="replace")
            df = pl.read_csv(
                StringIO(raw),
                infer_schema_length=0,
                ignore_errors=True,
                truncate_ragged_lines=True,
            )
            # janitor::clean_names
            df = df.rename({c: _clean_column_name(c) for c in df.columns})
            # Supprime les colonnes anonymes (commençant par "...")
            df = df.select([c for c in df.columns if not c.startswith("...")])
            # Supprime les guillemets résiduels (str_remove_all(x, '"'))
            df = df.with_columns(
                [pl.col(c).str.replace_all('"', "") for c in df.columns]
            )
            result[table_name] = df
        except Exception as e:
            log.debug("Impossible de lire %s : %s", f.name, e)

    return result


def get_gtfs_datasets_info() -> pl.DataFrame:
    """Interroge l'API transport.data.gouv.fr et retourne les métadonnées GTFS.

    Réplique fidèlement territoRy::get_dataset_info(format = "gtfs") :
    - Appel unique sur /api/datasets (liste plate, pas de pagination)
    - Filtre sur resources_format == "GTFS" (insensible à la casse)
    - Filtre resources_is_available == True
    - Dé-niche publisher → publisher_name / publisher_type
    - Dé-niche covered_area → categorie_geo (type du 1er élément)
    - Dé-niche sub_types et tags pour is_scolaire / is_saisonnier / is_agregat
    - Dé-niche resources_metadata pour start_date / end_date / stops_count
    """
    log.info("Getting dataset information from transport.data.gouv.fr")

    resp = requests.get("https://transport.data.gouv.fr/api/datasets", timeout=60)
    resp.raise_for_status()
    datasets_raw: list[dict] = resp.json()  # liste plate, pas de pagination

    rows = []
    seen_datagouv_ids: list[str] = []
    duplicates: list[str] = []

    for item in datasets_raw:
        publisher = item.get("publisher") or {}
        covered_area = item.get("covered_area") or []
        sub_types = item.get("sub_types") or []
        tags_list = item.get("tags") or []

        # Type géo = type du premier élément de covered_area
        categorie_geo = covered_area[0].get("type") if covered_area else None

        for row_idx, res in enumerate(item.get("resources") or []):
            # Filtre is_available (équivalent dplyr::filter(resources_is_available))
            if not res.get("is_available", True):
                continue

            # Filtre format == "GTFS" (insensible à la casse)
            fmt = (res.get("format") or "").strip()
            if fmt.upper() != "GTFS":
                continue

            metadata = res.get("metadata") or {}
            datagouv_id = res.get("datagouv_id")

            # Détection doublons (même logique que le warning R)
            if datagouv_id in seen_datagouv_ids:
                duplicates.append(datagouv_id)
            else:
                seen_datagouv_ids.append(datagouv_id)

            rows.append({
                # Identifiants dataset
                "id": item.get("id"),
                "datagouv_id": item.get("datagouv_id"),
                "title": item.get("title"),
                "slug": item.get("slug"),
                "type": item.get("type"),
                "page_url": item.get("page_url"),
                # Publisher (unnest_wider col = publisher, names_sep = "_")
                "publisher_name": publisher.get("name"),
                "publisher_type": publisher.get("type"),
                # Ressource (unnest_longer + unnest_wider col = resources, names_sep = "_")
                "resources_rowid": row_idx,
                "resources_id": res.get("id"),
                "resources_datagouv_id": datagouv_id,
                "resources_format": fmt,
                "resources_title": res.get("title"),
                "resources_type": res.get("type"),
                "resources_updated": res.get("updated"),
                "resources_url": res.get("url"),
                "resources_original_url": res.get("original_url"),
                "resources_page_url": res.get("page_url"),
                "resources_community_resource_publisher": res.get("community_resource_publisher"),
                "resources_schema_name": (res.get("schema") or {}).get("name"),
                "resources_schema_version": (res.get("schema") or {}).get("version"),
                "resources_features": res.get("features"),
                "resources_metadata": res.get("metadata"),       # conservé brut comme en R
                "resources_modes": res.get("modes"),
                "resources_filesize": res.get("filesize"),
                # Métadonnées dépliées (utilisées dans le script principal)
                "start_date": metadata.get("start_date"),
                "end_date": metadata.get("end_date"),
                "stops_count": metadata.get("stops_count"),
                # Couverture géographique
                "categorie_geo": categorie_geo,
                "covered_area": covered_area,                    # conservé brut
                # Sub-types et tags (utilisés pour is_scolaire / is_saisonnier / is_agregat)
                "sub_types": sub_types,
                "tags": tags_list,
                # Drapeaux calculés (équivalent dplyr::mutate dans le script R)
                "is_scolaire": any(st == "school" for st in sub_types),
                "is_saisonnier": any(st == "seasonal" for st in sub_types),
                "is_agregat": any(t == "agrégat_region" for t in tags_list),
                # Divers
                "community_resources": item.get("community_resources"),
                "aom": item.get("aom"),
                "licence": item.get("licence"),
                "legal_owners": item.get("legal_owners"),
                "created_at": item.get("created_at"),
                "updated": item.get("updated"),
            })

    if duplicates:
        log.warning(
            "Doublons trouvés pour les resources_datagouv_id suivants : %s",
            ", ".join(str(d) for d in set(duplicates)),
        )

    log.info("%d ressources GTFS disponibles après filtrage.", len(rows))
    return pl.DataFrame(rows)


def download_resource(
    slug: str,
    resources_id: str | int,
    resources_url: str,
    base_dir: Path,
    redownload: bool = False,
) -> Path:
    """Réplique territoRy::download_resource().

    Télécharge le zip dans base_dir/resources_id/gtfs.zip.
    Lève une exception en cas d'échec (pour permettre le retry dans download_resources).
    """
    destfile = base_dir / str(resources_id) / "gtfs.zip"
    destfile.parent.mkdir(parents=True, exist_ok=True)

    if destfile.exists() and not redownload:
        log.info("Fichier %s déjà présent, on passe.", slug)
        return destfile

    log.info("Téléchargement de %s depuis %s", slug, resources_url)
    r = requests.get(resources_url, timeout=15, stream=True)
    r.raise_for_status()
    with open(destfile, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
    return destfile


def download_resources(
    gtfs_datasets: pl.DataFrame,
    redownload: bool = False,
    timeout: int = 10,
) -> pl.DataFrame:
    """Réplique territoRy::download_resources().

    Itère sur gtfs_datasets, essaie resources_url puis resources_original_url en cas d'échec.
    Retourne un DataFrame avec colonnes : slug, destfile, downloaded.
    """
    socket.setdefaulttimeout(timeout)

    log.info("=== Downloading resources ===")
    results = []

    for row in gtfs_datasets.iter_rows(named=True):
        slug = row.get("slug", "")
        rid = row.get("resources_id")
        url = row.get("resources_url")
        original_url = row.get("resources_original_url")

        try:
            destfile = download_resource(slug, rid, url, BASE_DIR, redownload)
            results.append({"slug": slug, "destfile": str(destfile), "downloaded": True})
        except Exception:
            log.warning(
                "Échec téléchargement %s depuis %s. Nouvel essai avec original_url...", slug, url
            )
            try:
                destfile = download_resource(slug, rid, original_url, BASE_DIR, redownload)
                results.append({"slug": slug, "destfile": str(destfile), "downloaded": True})
            except Exception as e2:
                log.warning("Échec définitif pour %s : %s", slug, e2)
                results.append({"slug": slug, "destfile": None, "downloaded": False})

    dl_df = pl.DataFrame(results)
    failed = dl_df.filter(~pl.col("downloaded"))
    if len(failed) > 0:
        log.warning("Ressources non téléchargées : %s", ", ".join(failed["slug"].to_list()))
    return dl_df


def get_active_services(calendar: pl.DataFrame | None,
                        calendar_dates: pl.DataFrame | None,
                        fraicheur: date,
                        fraicheur_jour: str) -> pl.Series:
    """Retourne les service_id actifs à la date fraicheur."""
    active = pl.Series("service_id", [], dtype=pl.Utf8)

    if calendar is not None and len(calendar) > 0:
        if fraicheur_jour in calendar.columns:
            mask = (
                (calendar["start_date"].cast(pl.Utf8) <= str(fraicheur).replace("-", ""))
                & (calendar["end_date"].cast(pl.Utf8) >= str(fraicheur).replace("-", ""))
                & (calendar[fraicheur_jour] == "1")
            )
            active = calendar.filter(mask)["service_id"]

    if calendar_dates is not None and len(calendar_dates) > 0:
        date_str = str(fraicheur).replace("-", "")
        today_cd = calendar_dates.filter(pl.col("date") == date_str)
        if len(today_cd) > 0:
            added = today_cd.filter(pl.col("exception_type") == "1")["service_id"]
            removed = today_cd.filter(pl.col("exception_type") == "2")["service_id"]
            active = pl.concat([active, added]).unique()
            active = active.filter(~active.is_in(removed.to_list()))

    return active.unique()


def get_parent_station(stops: pl.DataFrame) -> pl.DataFrame:
    """Associe chaque stop_id à son parent_station (ou lui-même si aucun)."""
    if "parent_station" not in stops.columns:
        return stops.with_columns(pl.col("stop_id").alias("parent_station"))
    return stops.with_columns(
        pl.when(
            pl.col("parent_station").is_null() | (pl.col("parent_station") == "")
        )
        .then(pl.col("stop_id"))
        .otherwise(pl.col("parent_station"))
        .alias("parent_station")
    )


def map_route_type(rt: str | None) -> str:
    """Mappe un route_type GTFS en type de transport (bus, tramway, métro, train, autres)."""
    if rt is None:
        return "non renseigné"
    return ROUTE_TYPE_MAP.get(str(rt).strip(), "non renseigné")


# ─────────────────────────────────────────────
# Récupération des métadonnées
# ─────────────────────────────────────────────
log.info("=== Récupération des métadonnées GTFS ===")

try:
    gtfs_datasets = get_gtfs_datasets_info()
    gtfs_datasets = gtfs_datasets.with_columns(
        pl.lit(str(FRAICHEUR)).alias("resources_extract_date")
    )
    log.info("%d datasets GTFS disponibles sur l'API.", len(gtfs_datasets))
except Exception as e:
    log.error("Impossible de récupérer les métadonnées : %s", e)
    gtfs_datasets = pl.DataFrame()


# ─────────────────────────────────────────────
# Détection des dossiers GTFS locaux
# ─────────────────────────────────────────────
log.info("=== Téléchargement / détection des GTFS ===")

if REDOWNLOAD and len(gtfs_datasets) > 0:
    download_resources(gtfs_datasets, redownload=REDOWNLOAD, timeout=15)

all_dirs = [d for d in BASE_DIR.iterdir() if d.is_dir()] if BASE_DIR.exists() else []
gtfs_dirs = [d for d in all_dirs if (d / "gtfs.zip").exists()]
gtfs_dirs.sort()
log.info("%d dossiers GTFS à traiter.", len(gtfs_dirs))


# ─────────────────────────────────────────────
# Lecture et compilation
# ─────────────────────────────────────────────
log.info("=== Lecture et compilation des GTFS ===")

REQUIRED_FILES = ["stops", "routes", "trips", "stop_times"]
REQUIRED_COLS = {
    "stops": ["stop_id"],
    "routes": ["route_id"],
    "trips": ["trip_id", "route_id"],
    "stop_times": ["trip_id", "stop_id"],
}
ID_COLS = {
    "stops": ["stop_id"],
    "routes": ["route_id", "agency_id"],
    "trips": ["trip_id", "route_id", "service_id"],
    "stop_times": ["trip_id", "stop_id"],
    "agency": ["agency_id"],
    "calendar": ["service_id"],
    "calendar_dates": ["service_id"],
}

all_stops_data: list[pl.DataFrame] = []
result_extract: list[dict] = []
networks_list: list[dict] = []
n_datasets = len(gtfs_dirs)

for i, dataset_path in enumerate(gtfs_dirs, 1):
    dataset_id = dataset_path.name
    zip_path = dataset_path / "gtfs.zip"

    if not zip_path.exists():
        log.warning("Dataset %s introuvable, on passe.", dataset_id)
        result_extract.append({"dataset_id": dataset_id, "result": "not found"})
        continue

    # ── Extraction des .txt depuis le zip (territoRy::extract_gtfs_zip) ──
    gtfs_files_wanted = [
        "stop_times.txt", "stops.txt", "routes.txt", "trips.txt",
        "agency.txt", "calendar.txt", "calendars.txt", "calendar_dates.txt",
    ]
    extraction_res = extract_gtfs_zip(zip_path, files=gtfs_files_wanted)

    if not extraction_res["extracted"]:
        log.warning("Dataset %s : échec d'extraction, on passe.", dataset_id)
        result_extract.append({"dataset_id": dataset_id, "result": "failed extract"})
        continue

    # ── Lecture des .txt extraits (territoRy::read_gtfs_dataset) ──
    tables: dict[str, pl.DataFrame] = read_gtfs_dataset(
        dataset_path,
        files_to_keep=[
            "stop_times", "stops", "routes", "trips",
            "agency", "calendar", "calendars", "calendar_dates"
        ],
    )

    # Suppression des .txt temporaires après lecture (comme unlink() en R)
    for txt_file in dataset_path.glob("*.txt"):
        try:
            txt_file.unlink()
        except Exception:
            pass

    # Alias "calendars" → "calendar"
    if "calendars" in tables and "calendar" not in tables:
        tables["calendar"] = tables.pop("calendars")

    # ── Vérification fichiers obligatoires ──
    missing = [f for f in REQUIRED_FILES if f not in tables]
    if missing:
        log.warning(
            "Dataset %s : fichiers manquants (%s), on passe.", dataset_id, ", ".join(missing)
        )
        result_extract.append({"dataset_id": dataset_id, "result": f"missing {','.join(missing)}"})
        continue

    # ── Vérification colonnes obligatoires ──
    invalid = False
    for tbl, cols in REQUIRED_COLS.items():
        missing_cols = [c for c in cols if c not in tables[tbl].columns]
        if missing_cols:
            log.warning(
                "Dataset %s : colonnes manquantes dans %s → %s",
                dataset_id, tbl, ", ".join(missing_cols)
            )
            invalid = True
            break
    if invalid:
        result_extract.append({"dataset_id": dataset_id, "result": "invalid schema"})
        continue

    # ── Préfixage des identifiants ──
    for tbl, cols in ID_COLS.items():
        if tbl not in tables:
            continue
        exprs = []
        for col in cols:
            if col in tables[tbl].columns:
                exprs.append(
                    (pl.lit(dataset_id + "_") + pl.col(col)).alias(col)
                )
        if exprs:
            tables[tbl] = tables[tbl].with_columns(exprs)

    if "stops" in tables and "parent_station" in tables["stops"].columns:
        tables["stops"] = tables["stops"].with_columns(
            pl.when(
                pl.col("parent_station").is_not_null() & (pl.col("parent_station") != "")
            )
            .then(pl.lit(dataset_id + "_") + pl.col("parent_station"))
            .otherwise(pl.col("parent_station"))
            .alias("parent_station")
        )

    # ── Sauvegarde des tables brutes ──
    for tbl, df in tables.items():
        df_out = df.with_columns([
            pl.lit(dataset_id).alias("dataset_id"),
            pl.lit(str(FRAICHEUR)).alias("date_extraction"),
        ])
        out_dir = RAW_DIR / tbl
        out_dir.mkdir(parents=True, exist_ok=True)
        df_out.write_parquet(out_dir / f"{dataset_id}.parquet")

    # ── Résumé du réseau ──
    agency_names = None
    if "agency" in tables and "agency_name" in tables["agency"].columns:
        agency_names = ", ".join(tables["agency"]["agency_name"].drop_nulls().unique().to_list())

    cal_dates_series: list[str] = []
    if "calendar_dates" in tables and "date" in tables["calendar_dates"].columns:
        cal_dates_series = tables["calendar_dates"]["date"].drop_nulls().to_list()

    date_min = date_max = None
    if "calendar" in tables:
        cal = tables["calendar"]
        all_dates = (
            list(
                cal.get_column("start_date").drop_nulls().to_list()
                if "start_date" in cal.columns else []
            )
            + list(
                cal.get_column("end_date").drop_nulls().to_list()
                if "end_date" in cal.columns else []
            )
            + cal_dates_series
        )
        if all_dates:
            date_min = min(all_dates)
            date_max = max(all_dates)
    elif cal_dates_series:
        date_min = min(cal_dates_series)
        date_max = max(cal_dates_series)

    def _in_period(d: date, dmin, dmax) -> bool:
        if dmin is None or dmax is None:
            return True
        try:
            return str(d).replace("-", "") < str(dmin) or str(d).replace("-", "") > str(dmax)
        except Exception:
            return True

    networks_list.append({
        "resources_id": int(dataset_id) if dataset_id.isdigit() else dataset_id,
        "agency_name": agency_names,
        "date_min_observed": date_min,
        "date_max_observed": date_max,
        "hors_periode": _in_period(FRAICHEUR, date_min, date_max),
        "nb_stops_observed": len(tables.get("stops", pl.DataFrame())),
        "nb_routes_observed": len(tables.get("routes", pl.DataFrame())),
    })

    # ── Nettoyage de base ──
    stops_df = tables["stops"].filter(pl.col("stop_id").is_not_null()).unique()
    routes_df = tables["routes"].filter(pl.col("route_id").is_not_null()).unique()
    trips_df = tables["trips"].filter(
        pl.col("trip_id").is_not_null() | pl.col("route_id").is_not_null()
    ).unique()
    stop_times_df = tables["stop_times"].filter(pl.col("trip_id").is_not_null()).unique()

    if len(stops_df) == 0:
        log.warning("Dataset %s : aucun arrêt, on passe.", dataset_id)
        result_extract.append({"dataset_id": dataset_id, "result": "no stops"})
        continue

    # ── Services actifs ──
    active_services = get_active_services(
        tables.get("calendar"), tables.get("calendar_dates"), FRAICHEUR, FRAICHEUR_JOUR
    )

    if len(active_services) == 0:
        log.warning("Dataset %s : aucun service actif aujourd'hui, on passe.", dataset_id)
        result_extract.append({"dataset_id": dataset_id, "result": "no services"})
        continue

    try:
        # ── Filtrage trips/routes du jour ──
        trips_df = trips_df.filter(pl.col("service_id").is_in(active_services.to_list()))
        routes_df = routes_df.filter(pl.col("route_id").is_in(trips_df["route_id"].to_list()))

        # ── Corrections colonnes manquantes ──
        if "location_type" not in stops_df.columns:
            stops_df = stops_df.with_columns(pl.lit("0").alias("location_type"))
        stops_df = stops_df.with_columns(
            pl.when(pl.col("location_type") == "").then(pl.lit("0"))
            .otherwise(pl.col("location_type")).alias("location_type")
        )

        if "route_short_name" not in routes_df.columns:
            routes_df = routes_df.with_columns(pl.col("route_long_name").alias("route_short_name"))
        elif "route_long_name" not in routes_df.columns:
            routes_df = routes_df.with_columns(pl.col("route_short_name").alias("route_long_name"))
        # Si route_short_name vide, on prend route_long_name
        routes_df = routes_df.with_columns(
            pl.coalesce(["route_short_name", "route_long_name"]).alias("route_short_name")
        )

        agency_df = tables.get("agency", pl.DataFrame())
        if len(agency_df) == 0:
            agency_df = pl.DataFrame(
                {"agency_id": [""], "agency_name": [None], "agency_lang": [None]}
            )
        if "agency_id" not in agency_df.columns:
            agency_df = agency_df.with_columns(pl.lit("").alias("agency_id"))
        if "agency_lang" not in agency_df.columns:
            agency_df = agency_df.with_columns(pl.lit(None).cast(pl.Utf8).alias("agency_lang"))
        if "agency_id" not in routes_df.columns:
            routes_df = routes_df.with_columns(pl.lit(agency_df["agency_id"][0]).alias("agency_id"))

        if "parent_station" not in stops_df.columns:
            stops_df = stops_df.with_columns(pl.lit("").alias("parent_station"))
        if "pickup_type" not in stop_times_df.columns:
            stop_times_df = stop_times_df.with_columns(pl.lit("0").alias("pickup_type"))
        if "drop_off_type" not in stop_times_df.columns:
            stop_times_df = stop_times_df.with_columns(pl.lit("0").alias("drop_off_type"))

        # ── Parent station ──
        stations_df = get_parent_station(stops_df).select(["stop_id", "parent_station"])

        # Infos coords depuis la station parente
        stops_coords = stops_df.select(["stop_id", "stop_name", "stop_lat", "stop_lon"])
        stops_full = stations_df.join(
            stops_coords.rename({"stop_id": "parent_station"}),
            on="parent_station",
            how="left",
        )
        # Préférer le nom/coords du parent s'il existe
        for col in ["stop_name", "stop_lat", "stop_lon"]:
            if f"{col}_parent" in stops_full.columns:
                stops_full = stops_full.with_columns(
                    pl.coalesce([f"{col}_parent", col]).alias(col)
                ).drop(f"{col}_parent")

        # ── Sélection colonnes routes ──
        route_cols = [
            c for c in ["route_id", "route_type", "route_short_name",
                        "route_long_name", "agency_id"]
            if c in routes_df.columns
        ]
        routes_sel = routes_df.select(route_cols).unique()
        routes_sel = routes_sel.with_columns(
            pl.when(pl.col("route_short_name") == "")
            .then(pl.col("route_long_name"))
            .otherwise(pl.col("route_short_name"))
            .alias("route_short_name")
        )

        trips_sel = trips_df.select(["route_id", "trip_id"]).unique()

        st_cols = [
            c for c in ["trip_id", "stop_id", "stop_sequence",
                        "arrival_time", "pickup_type", "drop_off_type"]
            if c in stop_times_df.columns
        ]
        st_sel = stop_times_df.select(st_cols).unique()
        st_sel = st_sel.with_columns([
            pl.col("pickup_type").fill_null("0"),
            pl.col("drop_off_type").fill_null("0"),
        ])

        # ── Jointures ──
        j1 = stops_full.join(st_sel, on="stop_id", how="inner")
        j2 = j1.join(trips_sel, on="trip_id", how="inner")
        agency_sel = agency_df.select(
            [c for c in ["agency_id", "agency_name", "agency_lang"] if c in agency_df.columns]
        ).unique()
        j3 = (
            routes_sel.join(agency_sel, on="agency_id", how="left")
            .join(j2, on="route_id", how="inner")
        )

        # ── Transport à la demande ──
        tad = j3.group_by("route_id").agg([
            (pl.col("drop_off_type") == "2").all().alias("ligne_ad"),
            (pl.col("drop_off_type") == "2").any().alias("ligne_ad_partiel"),
        ])
        j3 = j3.join(tad, on="route_id", how="left")
        j3 = j3.with_columns(
            pl.when(pl.col("ligne_ad")).then(pl.lit(False))
            .otherwise(pl.col("ligne_ad_partiel")).alias("ligne_ad_partiel")
        )

        # ── Une ligne par parent_station x route ──
        arret_route = j3.filter(pl.col("trip_id").is_not_null()).group_by(
            ["parent_station", "route_id"]
        ).first()

        # ── Fréquence PPM (7h-9h) ──
        def extract_hour(s: pl.Series) -> pl.Series:
            """Extrait l'heure d'une colonne arrival_time (HH:MM:SS) et retourne un Int"""
            return s.str.split(":").list.get(0).cast(pl.Int32, strict=False)

        if "arrival_time" in j3.columns:
            ppm_df = j3.filter(
                (pl.col("pickup_type") == "0") | (pl.col("drop_off_type") == "0")
            )
            ppm_df = ppm_df.with_columns(
                extract_hour(pl.col("arrival_time")).alias("_hour")
            ).filter(pl.col("_hour").is_between(7, 8))

            if len(ppm_df) > 0:
                freq_ppm = (
                    ppm_df.group_by(
                        ["stop_id", "parent_station", "route_id", "route_type", "stop_sequence"]
                    )
                    .agg(pl.len().alias("N"))
                    .sort("N", descending=True)
                    .group_by(["parent_station", "route_id", "route_type"])
                    .first()
                    .select(["parent_station", "route_id", "route_type", "N"])
                    .rename({"N": "freq_ppm_max"})
                )
                arret_route = arret_route.join(
                    freq_ppm, on=["parent_station", "route_id", "route_type"], how="left"
                )
            else:
                arret_route = arret_route.with_columns(pl.lit(0).alias("freq_ppm_max"))
        else:
            arret_route = arret_route.with_columns(pl.lit(0).alias("freq_ppm_max"))

        # ── Type de route ──
        arret_route = arret_route.with_columns(
            pl.col("route_type").map_elements(map_route_type, return_dtype=pl.Utf8)
            .alias("route_type")
        )
        arret_route = arret_route.with_columns(
            pl.when(pl.col("ligne_ad"))
            .then(pl.col("route_type") + pl.lit(" TAD"))
            .otherwise(pl.col("route_type"))
            .alias("route_type")
        )

        # Priorité mode
        priority_map = {"train": 1, "métro": 2, "tramway": 3, "bus": 4, "bus TAD": 4}
        arret_route = arret_route.with_columns(
            pl.col("route_type").replace_strict(priority_map, default=999).alias("rang_route_type")
        )

        # ── Nettoyage noms d'arrêts ──
        arret_route = arret_route.with_columns(
            pl.col("stop_name").map_elements(
                lambda x: reduce_stop_name(x) if x else x, return_dtype=pl.Utf8
            ).alias("stop_name_red")
        )

        # Ligne principale (rang 1 = mode le plus prioritaire par route)
        arret_route = arret_route.sort("rang_route_type")
        ligne_princ = (
            arret_route.select(["route_short_name", "rang_route_type"])
            .unique()
            .with_columns(
                pl.col("rang_route_type").rank(method="dense")
                .over(["route_short_name"]).alias("ligne_princ")
            )
        )
        arret_route = arret_route.join(
            ligne_princ,
            on=["route_short_name", "rang_route_type"],
            how="left"
        )

        # Table finale
        arret_route_nom_red = (
            arret_route.filter(pl.col("ligne_princ") == 1)
            .group_by(["route_short_name", "stop_name_red", "parent_station"])
            .first()
        )

        keep_cols = [c for c in [
            "parent_station", "stop_id", "stop_name", "stop_name_red",
            "stop_lat", "stop_lon", "route_id", "route_type",
            "route_short_name", "route_long_name",
            "agency_id", "agency_name", "agency_lang",
            "freq_ppm_max", "ligne_ad", "ligne_ad_partiel",
        ] if c in arret_route_nom_red.columns]

        stations_routes = (
            arret_route_nom_red.select(keep_cols)
            .drop("stop_id").rename({"parent_station": "stop_id"})
        )

        # Formatage final
        processed = stations_routes.with_columns([
            pl.lit(str(FRAICHEUR)).alias("date_extraction"),
            pl.lit(dataset_id).alias("dataset_id"),
            pl.col("stop_lat").cast(pl.Float64, strict=False).alias("latitude"),
            pl.col("stop_lon").cast(pl.Float64, strict=False).alias("longitude"),
        ])
        if "stop_lat" in processed.columns:
            processed = processed.drop(["stop_lat", "stop_lon"])

        # stop_id_red et route_id_red
        processed = processed.with_columns([
            (
                pl.col("stop_id").str.extract(r"(\w+)$")
                .fill_null(pl.col("stop_name_red")).alias("stop_id_red")
            ),
            (
                pl.col("route_id").str.extract(r"(\w+)$")
                .fill_null(pl.col("route_short_name")).alias("route_id_red")
            ),
        ])

        # Mise en title case
        for col in ["stop_name", "route_long_name"]:
            if col in processed.columns:
                processed = processed.with_columns(
                    pl.col(col).str.to_titlecase().str.strip_chars().alias(col)
                )

        processed = processed.filter(
            pl.col("latitude").is_not_null() | pl.col("route_id").is_not_null()
        )
        if "freq_ppm_max" in processed.columns:
            processed = processed.with_columns(pl.col("freq_ppm_max").fill_null(0))

        log.info(
            "Dataset %s (%d/%d) : %d arrêts traités.", dataset_id, i, n_datasets, len(processed)
        )
        all_stops_data.append(processed)
        result_extract.append({"dataset_id": dataset_id, "result": "success"})

    except Exception as e:
        log.warning("Dataset %s : erreur de traitement (%s), on passe.", dataset_id, e)
        log.warning(traceback.format_exc())
        result_extract.append({"dataset_id": dataset_id, "result": "processing error"})

    del tables
    gc.collect()


# ─────────────────────────────────────────────
# Consolidation des tables brutes
# ─────────────────────────────────────────────
log.info("=== Consolidation des tables GTFS brutes ===")

TABLES_GTFS = ["stops", "routes", "trips", "stop_times", "agency", "calendar", "calendar_dates"]

for tbl in TABLES_GTFS:
    tbl_dir = RAW_DIR / tbl
    fichiers = list(tbl_dir.glob("*.parquet")) if tbl_dir.exists() else []
    if not fichiers:
        log.warning("Table %s : aucun fichier trouvé, on passe.", tbl)
        continue
    dfs = [pl.read_parquet(f) for f in fichiers]
    consolidated = pl.concat(dfs, how="diagonal")
    out_path = CONSOLIDATED_DIR / f"{tbl}.parquet"
    consolidated.write_parquet(out_path)
    log.info("Table %s consolidée (%d datasets).", tbl, len(fichiers))

# ─────────────────────────────────────────────
# Rapport des réseaux
# ─────────────────────────────────────────────
if networks_list:
    networks_df = pl.DataFrame(networks_list)
    if len(gtfs_datasets) > 0 and "resources_id" in gtfs_datasets.columns:
        gtfs_datasets_info_out = gtfs_datasets.join(networks_df, on="resources_id", how="left")
    else:
        gtfs_datasets_info_out = networks_df

    date_str = str(FRAICHEUR).replace("-", "")
    gtfs_datasets_info_out.write_parquet(
        DATA_DIR / "transport.data.gouv.fr" / f"{date_str}_gtfs_datasets_info.parquet"
    )

# ─────────────────────────────────────────────
# Sauvegarde des résultats
# ─────────────────────────────────────────────
log.info("=== Sauvegarde ===")

result_df = pl.DataFrame(result_extract) if result_extract else pl.DataFrame({"dataset_id": [], "result": []})
date_str = str(FRAICHEUR).replace("-", "")
result_df.write_parquet(
    DATA_DIR / "transport.data.gouv.fr" / f"{date_str}_resultats_extraction.parquet"
)

# ── Compilation finale ──
if all_stops_data:
    all_stops_data = [
        df.with_columns(pl.col("freq_ppm_max").cast(pl.Int64))
        if "freq_ppm_max" in df.columns else df
        for df in all_stops_data
    ]
    all_stops = pl.concat(all_stops_data, how="diagonal")
else:
    all_stops = pl.DataFrame()
    log.warning("Aucun arrêt compilé.")

if len(all_stops) > 0:
    # Nettoyages finaux
    all_stops = all_stops.filter(pl.col("latitude").is_not_null() | pl.col("route_id").is_null())

    if "agency_lang" in all_stops.columns:
        all_stops = all_stops.with_columns(
            pl.col("agency_lang").str.to_uppercase()
            .str.replace("^$", None)
            .str.replace("FR-FR", "FR")
            .alias("agency_lang")
        )

    if "freq_ppm_max" in all_stops.columns:
        all_stops = all_stops.with_columns(pl.col("freq_ppm_max").fill_null(0))
    all_stops = all_stops.unique()

    # Vérification route_type "non renseigné"
    if "route_type" in all_stops.columns:
        nb_nr = all_stops.filter(pl.col("route_type") == "non renseigné").height
        if nb_nr < len(all_stops) * 0.001:
            all_stops = all_stops.with_columns(
                pl.when(pl.col("route_type") == "non renseigné")
                .then(pl.lit("autres"))
                .otherwise(pl.col("route_type"))
                .alias("route_type")
            )

    # Déduplication
    sort_col = "freq_ppm_max" if "freq_ppm_max" in all_stops.columns else all_stops.columns[0]

    for group_cols in [
        ["stop_name_red", "route_short_name", "route_type", "latitude", "longitude"],
        ["stop_id", "stop_name_red", "route_id", "agency_id"],
    ]:
        valid_group = [c for c in group_cols if c in all_stops.columns]
        if valid_group and sort_col in all_stops.columns:
            all_stops = (
                all_stops.sort(sort_col, descending=True)
                .group_by(valid_group)
                .first()
            )

    # Dédup par coordonnées arrondies
    if "latitude" in all_stops.columns and "longitude" in all_stops.columns:
        all_stops = all_stops.with_columns([
            pl.col("latitude").round(3).alias("arrond_lat"),
            pl.col("longitude").round(3).alias("arrond_lon"),
        ])
        group_rnd = [
            c for c in ["stop_name_red", "route_short_name",
                        "route_type", "arrond_lat", "arrond_lon"]
            if c in all_stops.columns]
        if group_rnd and sort_col in all_stops.columns:
            all_stops = (
                all_stops.sort(sort_col, descending=True)
                .group_by(group_rnd)
                .first()
                .drop(["arrond_lat", "arrond_lon"])
            )

    out_path = DATA_DIR / f"all_stops_data_{date_str}.parquet"
    all_stops.write_parquet(out_path)

    nb_success = sum(1 for r in result_extract if r["result"] == "success")
    log.info("Terminé ! %d arrêts compilés.", len(all_stops))
    log.info("Fichier sauvegardé : %s", out_path)
    log.info(
        "Rapport d'extraction : %d/%d datasets traités avec succès.",
        nb_success, len(result_extract)
    )
