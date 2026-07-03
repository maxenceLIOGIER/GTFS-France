import gc
import traceback
import logging
from datetime import date
import polars as pl

from config import (
    BASE_DIR, FRAICHEUR, REDOWNLOAD, FRAICHEUR_JOUR,
    RAW_DIR, CONSOLIDATED_DIR, DATA_DIR
)
from utils_gtfs import (
    get_gtfs_datasets_info,
    download_resources,
    extract_gtfs_zip,
    get_parent_station,
    read_gtfs_dataset,
    get_active_services,
    map_route_type,
    reduce_stop_name,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


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

    out_path = CONSOLIDATED_DIR / f"{tbl}.parquet"
    try:
        if tbl == "stop_times":
            lfs = [pl.scan_parquet(str(f)) for f in fichiers]
            lf = pl.concat(lfs, how="diagonal_relaxed")
            lf.sink_parquet(out_path)
        else:
            dfs = [pl.read_parquet(f) for f in fichiers]
            consolidated = pl.concat(dfs, how="diagonal")
            consolidated.write_parquet(out_path)
            del dfs, consolidated

        log.info("Table %s consolidée (%d datasets).", tbl, len(fichiers))
        gc.collect()
    except Exception as e:
        log.error("Table %s : échec de consolidation (%s).", tbl, e)
        log.error(traceback.format_exc())


# ─────────────────────────────────────────────
# Persistance des données brutes pour le rapport
# ─────────────────────────────────────────────
log.info("=== Persistance des données pour le rapport des réseaux ===")

date_str = str(FRAICHEUR).replace("-", "")
rapport_dir = DATA_DIR / "transport.data.gouv.fr"

if networks_list:
    networks_df = pl.DataFrame(networks_list)
    networks_df.write_parquet(rapport_dir / f"{date_str}_networks_raw.parquet")
    log.info("networks_df persisté (%d lignes).", len(networks_df))
else:
    log.warning("networks_list est vide, rien à persister pour le rapport.")

if len(gtfs_datasets) > 0:
    gtfs_datasets.write_parquet(rapport_dir / f"{date_str}_gtfs_datasets_raw.parquet")
    log.info("gtfs_datasets persisté (%d lignes).", len(gtfs_datasets))
else:
    log.warning("gtfs_datasets est vide, rien à persister pour le rapport.")


# ─────────────────────────────────────────────
# Sauvegarde des résultats
# ─────────────────────────────────────────────
log.info("=== Sauvegarde ===")

result_df = (
    pl.DataFrame(result_extract) if result_extract
    else pl.DataFrame({"dataset_id": [], "result": []})
)
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
