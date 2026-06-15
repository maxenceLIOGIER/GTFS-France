"""
Fonctions utilitaires pour le pipeline GTFS France.
Réplique les fonctions du package R territoRy.
"""

import re
import zipfile
import unicodedata
import logging
import socket
from datetime import date
from io import StringIO
from pathlib import Path

import polars as pl
import requests

from config import (
    ABBREV_MAP,
    BASE_DIR,
    ROUTE_TYPE_MAP,
    STOPWORDS_FR,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Nettoyage des noms d'arrêts
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


# ─────────────────────────────────────────────
# I/O GTFS — réplique territoRy
# ─────────────────────────────────────────────

def make_local_resource_path(base_dir: Path, resources_id: str | int) -> Path:
    """Réplique territoRy::make_local_resource_path() : base_dir / resources_id."""
    return base_dir / str(resources_id)


def extract_gtfs_zip(gtfs_zip_file: Path, files: list[str] | None = None) -> dict:
    """Extrait les fichiers .txt d'un zip GTFS dans son dossier parent.

    Réplique territoRy::extract_gtfs_zip() :
    - Liste les fichiers du zip
    - Correspond les noms demandés via pmatch (basename)
    - Extrait avec junkpaths=True (flatten des sous-dossiers)
    - Retourne {"files": "a.txt, b.txt, ...", "extracted": bool}
    """
    destdir = gtfs_zip_file.parent

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

    # Extraction avec flatten (junkpaths) : tous les .txt à la racine de destdir
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

    return {
        "files": ", ".join(extracted_names),
        "extracted": len(extracted_names) > 0,
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
    gtfs_dir: Path,
    files_to_keep: list[str] | None = None,
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
        try:
            raw = f.read_bytes().decode("utf-8-sig", errors="replace")
            df = pl.read_csv(
                StringIO(raw),
                infer_schema_length=0,
                ignore_errors=True,
                truncate_ragged_lines=True,
            )
            df = df.rename({c: _clean_column_name(c) for c in df.columns})
            df = df.select([c for c in df.columns if not c.startswith("...")])
            df = df.with_columns(
                [pl.col(c).str.replace_all('"', "") for c in df.columns]
            )
            result[f.stem] = df
        except Exception as e:
            log.debug("Impossible de lire %s : %s", f.name, e)

    return result


# ─────────────────────────────────────────────
# API transport.data.gouv.fr
# ─────────────────────────────────────────────

def get_gtfs_datasets_info() -> pl.DataFrame:
    """Interroge l'API transport.data.gouv.fr et retourne les métadonnées GTFS.

    Réplique territoRy::get_dataset_info(format = "gtfs") :
    - Appel unique sur /api/datasets (liste plate, pas de pagination)
    - Filtre resources_is_available == True
    - Filtre resources_format == "GTFS" (insensible à la casse)
    - Détection des doublons resources_datagouv_id
    """
    log.info("Getting dataset information from transport.data.gouv.fr")

    resp = requests.get("https://transport.data.gouv.fr/api/datasets", timeout=60)
    resp.raise_for_status()
    datasets_raw: list[dict] = resp.json()

    rows = []
    seen_datagouv_ids: list[str] = []
    duplicates: list[str] = []

    for item in datasets_raw:
        publisher = item.get("publisher") or {}
        covered_area = item.get("covered_area") or []
        sub_types = item.get("sub_types") or []
        tags_list = item.get("tags") or []
        categorie_geo = covered_area[0].get("type") if covered_area else None

        for row_idx, res in enumerate(item.get("resources") or []):
            if not res.get("is_available", True):
                continue
            fmt = (res.get("format") or "").strip()
            if fmt.upper() != "GTFS":
                continue

            metadata = res.get("metadata") or {}
            datagouv_id = res.get("datagouv_id")

            if datagouv_id in seen_datagouv_ids:
                duplicates.append(datagouv_id)
            else:
                seen_datagouv_ids.append(datagouv_id)

            rows.append({
                "id": item.get("id"),
                "datagouv_id": item.get("datagouv_id"),
                "title": item.get("title"),
                "slug": item.get("slug"),
                "type": item.get("type"),
                "page_url": item.get("page_url"),
                "publisher_name": publisher.get("name"),
                "publisher_type": publisher.get("type"),
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
                "resources_metadata": res.get("metadata"),
                "resources_modes": res.get("modes"),
                "resources_filesize": res.get("filesize"),
                "start_date": metadata.get("start_date"),
                "end_date": metadata.get("end_date"),
                "stops_count": metadata.get("stops_count"),
                "categorie_geo": categorie_geo,
                "covered_area": covered_area,
                "sub_types": sub_types,
                "tags": tags_list,
                "is_scolaire": any(st == "school" for st in sub_types),
                "is_saisonnier": any(st == "seasonal" for st in sub_types),
                "is_agregat": any(t == "agrégat_region" for t in tags_list),
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


# ─────────────────────────────────────────────
# Téléchargement — réplique territoRy
# ─────────────────────────────────────────────

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
    destfile = make_local_resource_path(base_dir, resources_id) / "gtfs.zip"
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


# ─────────────────────────────────────────────
# Logique métier GTFS
# ─────────────────────────────────────────────

def get_active_services(
    calendar: pl.DataFrame | None,
    calendar_dates: pl.DataFrame | None,
    fraicheur: date,
    fraicheur_jour: str,
) -> pl.Series:
    """Retourne les service_id actifs à la date fraicheur."""
    active = pl.Series("service_id", [], dtype=pl.Utf8)
    date_str = str(fraicheur).replace("-", "")

    if calendar is not None and len(calendar) > 0:
        if fraicheur_jour in calendar.columns:
            mask = (
                (calendar["start_date"] <= date_str)
                & (calendar["end_date"] >= date_str)
                & (calendar[fraicheur_jour] == "1")
            )
            active = calendar.filter(mask)["service_id"]

    if calendar_dates is not None and len(calendar_dates) > 0:
        today_cd = calendar_dates.filter(pl.col("date") == date_str)
        if len(today_cd) > 0:
            added = today_cd.filter(pl.col("exception_type") == "1")["service_id"]
            removed = today_cd.filter(pl.col("exception_type") == "2")["service_id"]
            active = pl.concat([active, added]).unique()
            active = active.filter(~active.is_in(removed.to_list()))

    return active.unique()


def get_parent_station(stops: pl.DataFrame) -> pl.DataFrame:
    """Associe chaque stop_id à son parent_station (ou lui-même si aucun).

    Réplique territoRy / gtfstools::get_parent_station() (un niveau).
    """
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
    """Mappe un route_type GTFS en catégorie (bus, tramway, métro, train, autres)."""
    if rt is None:
        return "non renseigné"
    return ROUTE_TYPE_MAP.get(str(rt).strip(), "non renseigné")
