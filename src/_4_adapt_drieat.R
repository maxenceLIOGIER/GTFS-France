###  Agrégation de tous les GTFS disponibles sur transport.data.gouv.fr

library(territoRy)
library(dplyr)
library(tidyr)
library(data.table)
library(purrr)
library(arrow)
library(here)
library(fs)
library(glue)
library(lubridate)
library(gtfstools)
library(stringr)
library(stringi)
library(stopwords)


### Paramètres

# fraicheur <- Sys.Date()
fraicheur <- as.Date("2026-06-01")
fraicheur_jour <- wday(fraicheur, label = TRUE, abbr = FALSE, locale = "en") |> tolower()
redownload <- FALSE # FALSE pour réutiliser un téléchargement existant

set.seed(2026)

data_dir <- ifelse(Sys.getenv("DATA_DIR") != "", Sys.getenv("DATA_DIR"), file.path(getwd(), "data"))
base_dir <- normalizePath(
    file.path(data_dir, "transport.data.gouv.fr", fraicheur),
    mustWork = FALSE
)
fs::dir_create(base_dir)
raw_dir <- file.path(data_dir, "transport.data.gouv.fr", "raw_tables", fraicheur)
fs::dir_create(raw_dir)

### Récupération des métadonnées GTFS depuis l'API

cli::cli_h1("Récupération des métadonnées GTFS")

gtfs_datasets_info <- territoRy::get_dataset_info(format = "gtfs") |>
    tidyr::unnest_wider(col = resources_metadata) |>
    tidyr::unnest_wider(col = sub_types, names_sep = "_") |>
    tidyr::unnest_wider(col = tags, names_sep = "_") |>
    dplyr::mutate(categorie_geo = purrr::map_chr(covered_area, \(x) purrr::pluck(x, 1, "type")))

gtfs_datasets <- gtfs_datasets_info |>
    dplyr::select(
        resources_id, categorie_geo,
        publisher_name,
        resources_title, title, slug,
        start_date, end_date, stops_count,
        dplyr::starts_with("sub_types"),
        dplyr::starts_with("tags"),
        datagouv_id, resources_datagouv_id,
        resources_original_url, resources_page_url, resources_url
    ) |>
    dplyr::arrange(resources_id) |>
    dplyr::distinct(resources_id, .keep_all = TRUE) |>
    dplyr::mutate(
        resources_extract_date = fraicheur,
        is_scolaire = dplyr::if_any(dplyr::starts_with("sub_types"), ~ . == "school"),
        is_saisonnier = dplyr::if_any(dplyr::starts_with("sub_types"), ~ . == "seasonal"),
        is_agregat = dplyr::if_any(dplyr::starts_with("tags"), ~ . == "agrégat_region"),
        .before = datagouv_id
    ) |>
    dplyr::mutate(
        data_path = purrr::map_chr(
            resources_id,
            \(x) territoRy::make_local_resource_path(base_dir = base_dir, resources_id = x)
        ),
        .before = 1
    )

cli::cli_alert_success("{nrow(gtfs_datasets)} datasets GTFS disponibles sur l'API.")


### Téléchargement des fichiers GTFS

cli::cli_h1("Téléchargement des GTFS")

if (redownload) {
    download_res <- territoRy::download_resources(
        gtfs_datasets,
        redownload = redownload,
        timeout = 15
    )
    gtfs_dirs <- normalizePath(fs::path_dir(download_res$destfile)) |> sort()
} else {
    cli::cli_alert_info("Pas de téléchargement — utilisation des fichiers existants.")
    all_dirs <- normalizePath(list.dirs(base_dir, full.names = TRUE, recursive = FALSE))
    gtfs_dirs <- all_dirs[file.exists(file.path(all_dirs, "gtfs.zip"))]
}

cli::cli_alert_info("{length(gtfs_dirs)} dossiers GTFS à traiter.")


### Lecture et compilation de chaque GTFS

cli::cli_h1("Lecture et compilation des GTFS")

all_stops_data <- data.table::data.table()
result_extract <- data.table::data.table()
networks_list <- vector("list", length(gtfs_dirs))
n_datasets <- length(gtfs_dirs)

for (i in seq_along(gtfs_dirs)) {
    dataset <- gtfs_dirs[i]
    dataset_id <- basename(dataset)

    # --- Vérification existence du dossier ---
    if (!dir.exists(dataset)) {
        cli::cli_alert_warning("Dataset {.key {dataset_id}} introuvable, on passe.")
        result_extract <- rbind(result_extract, list(dataset_id = dataset_id, result = "not found"))
        next
    }

    # --- Extraction des .txt depuis le zip ---
    extraction_res <- tryCatch(
        territoRy::extract_gtfs_zip(
            file.path(dataset, "gtfs.zip"),
            files = paste0(c(
                "stop_times", "stops", "routes", "trips", "agency",
                "calendar", "calendars", "calendar_dates"
            ), ".txt")
        ),
        error = function(e) list(extracted = FALSE)
    )

    if (!extraction_res$extracted) {
        cli::cli_alert_warning("Dataset {.key {dataset_id}} : échec d'extraction, on passe.")
        result_extract <- rbind(
            result_extract, list(dataset_id = dataset_id, result = "failed extract")
        )
        next
    }

    # --- Vérification des fichiers obligatoires ---
    extracted_files <- gsub(".txt", "", unlist(strsplit(extraction_res$files, ", ")))
    if (!all(c("stops", "routes", "trips", "stop_times") %in% extracted_files)) {
        missing <- setdiff(c("stops", "routes", "trips", "stop_times"), extracted_files)
        cli::cli_alert_warning(
            "Dataset {.key {dataset_id}} : fichiers manquants ({paste(missing, collapse=', ')}), on passe."
        )
        result_extract <- rbind(
            result_extract,
            list(dataset_id = dataset_id, result = paste("missing", paste(missing, collapse = ",")))
        )
        next
    }

    # --- Lecture ---
    initial_data <- tryCatch(
        {
            raw <- tryCatch(
                territoRy::read_gtfs_dataset(dataset, na.strings = "", header = TRUE),
                error = function(e) {
                    territoRy::read_gtfs_dataset(
                        dataset,
                        na.strings = "", quote = "", header = TRUE
                    )
                }
            )
            parsed <- purrr::map(
                raw,
                \(df) df |> dplyr::mutate(
                    dplyr::across(dplyr::everything(), \(x) stringr::str_remove_all(x, "\""))
                )
            ) |>
                gtfstools::as_dt_gtfs() |>
                gtfstools:::convert_from_standard()

            if ("calendars" %in% names(parsed)) {
                names(parsed)[names(parsed) == "calendars"] <- "calendar"
                parsed <- gtfstools:::convert_from_standard(parsed)
            }
            parsed
        },
        error = function(e) {
            cli::cli_alert_warning(
                "Dataset {.key {dataset_id}} : erreur de lecture ({conditionMessage(e)}), on passe."
            )
            NULL
        }
    )

    unlink(file.path(dataset, "*.txt"))

    if (is.null(initial_data)) {
        result_extract <- rbind(
            result_extract, list(dataset_id = dataset_id, result = "read error")
        )
        next
    }


    # Vérification des colonnes obligatoires
    required_cols <- list(
        stops = c("stop_id"),
        routes = c("route_id"),
        trips = c("trip_id", "route_id"),
        stop_times = c("trip_id", "stop_id")
    )

    invalid_gtfs <- FALSE
    for (tbl in names(required_cols)) {
        if (!tbl %in% names(initial_data)) {
            cli::cli_alert_warning(
                "Dataset {.key {dataset_id}} : table {.val {tbl}} absente."
            )
            invalid_gtfs <- TRUE
            break
        }

        missing_cols <- setdiff(required_cols[[tbl]], colnames(initial_data[[tbl]]))
        if (length(missing_cols) > 0) {
            cli::cli_alert_warning(
                paste0(
                    "Dataset ", dataset_id,
                    " : colonnes manquantes dans ", tbl,
                    " -> ", paste(missing_cols, collapse = ", ")
                )
            )
            invalid_gtfs <- TRUE
            break
        }
    }

    if (invalid_gtfs) {
        result_extract <- rbind(
            result_extract,
            list(dataset_id = dataset_id, result = "invalid schema")
        )
        next
    }

    # --- Préfixage des identifiants par dataset_id ---
    # Colonnes d'identifiants directs
    direct_id_cols <- list(
        stops = c("stop_id"),
        routes = c("route_id", "agency_id"),
        trips = c("trip_id", "route_id", "service_id", "shape_id"),
        stop_times = c("trip_id", "stop_id"),
        agency = c("agency_id"),
        calendar = c("service_id"),
        calendar_dates = c("service_id"),
        shapes = c("shape_id")
    )
    # parent_station est une référence vers stop_id, on la traite uniquement quand non vide
    for (tbl in names(initial_data)) {
        cols <- intersect(direct_id_cols[[tbl]], colnames(initial_data[[tbl]]))
        for (col in cols) {
            set(initial_data[[tbl]],
                j = col,
                value = paste0(dataset_id, "_", initial_data[[tbl]][[col]])
            )
        }
        if (tbl == "stops" && "parent_station" %in% colnames(initial_data$stops)) {
            initial_data$stops[
                !is.na(parent_station) & parent_station != "",
                parent_station := paste0(dataset_id, "_", parent_station)
            ]
        }
    }


    # --- Résumé du réseau ---
    agency_names <- if ("agency" %in% names(initial_data) && nrow(initial_data$agency) > 0) {
        paste0(unique(initial_data$agency$agency_name), collapse = ", ")
    } else {
        NA_character_
    }

    cal_dates <- if (
        "calendar_dates" %in% names(initial_data) && nrow(initial_data$calendar_dates) > 0
    ) {
        initial_data$calendar_dates$date
    } else {
        as.Date(character(0))
    }

    cal_range <- if ("calendar" %in% names(initial_data) && nrow(initial_data$calendar) > 0) {
        list(
            min = min(c(initial_data$calendar$start_date, cal_dates), na.rm = TRUE),
            max = max(c(initial_data$calendar$end_date, cal_dates), na.rm = TRUE)
        )
    } else if (length(cal_dates) > 0) {
        list(min = min(cal_dates, na.rm = TRUE), max = max(cal_dates, na.rm = TRUE))
    } else {
        list(min = NA_real_, max = NA_real_)
    }

    dataset_info <- dplyr::filter(gtfs_datasets, resources_id == as.integer(dataset_id))
    resource_url <- dplyr::pull(dataset_info, resources_url) |> dplyr::first()
    dataset_title <- dplyr::pull(dataset_info, title) |> dplyr::first()

    networks_list[[i]] <- data.table::data.table(
        resources_id = as.integer(dataset_id),
        agency_name = agency_names,
        date_min_observed = as.character(cal_range$min),
        date_max_observed = as.character(cal_range$max),
        hors_periode = fraicheur < cal_range$min | fraicheur > cal_range$max,
        nb_stops_observed = nrow(initial_data$stops),
        nb_routes_observed = nrow(initial_data$routes)
    )


    # --- Sauvegarde des tables brutes (avant tout filtrage) ---
    for (tbl in names(initial_data)) {
        tbl_dt <- data.table::as.data.table(initial_data[[tbl]])
        tbl_dt[, dataset_id := dataset_id]
        tbl_dt[, date_extraction := as.character(fraicheur)]
        out_tbl_dir <- file.path(raw_dir, tbl)
        fs::dir_create(out_tbl_dir)
        arrow::write_parquet(tbl_dt, file.path(out_tbl_dir, glue("{dataset_id}.parquet")))
    }

    # --- Nettoyage de base ---
    initial_data$stops <- unique(initial_data$stops[!is.na(stop_id)])
    initial_data$routes <- unique(initial_data$routes[!is.na(route_id)])
    initial_data$trips <- unique(initial_data$trips[!is.na(trip_id) | !is.na(route_id)])
    initial_data$stop_times <- unique(initial_data$stop_times[!is.na(trip_id)])

    if (nrow(initial_data$stops) == 0L) {
        cli::cli_alert_warning("Dataset {.key {dataset_id}} : aucun arrêt, on passe.")
        result_extract <- rbind(result_extract, list(dataset_id = dataset_id, result = "no stops"))
        next
    }

    # --- Services actifs pour la date du jour ---
    if (!"calendar" %in% names(initial_data)) {
        services_actifs <- initial_data$calendar_dates[
            date == fraicheur & exception_type == 1, "service_id"
        ]
    } else {
        vars_cols <- c("service_id", fraicheur_jour)
        services_actifs <- initial_data$calendar[
            !is.na(service_id) & data.table::between(
                fraicheur, start_date, end_date,
                NAbounds = FALSE
            ),
            ..vars_cols
        ] |>
            dplyr::filter(.data[[fraicheur_jour]] == "1") |>
            dplyr::select(service_id)

        if ("calendar_dates" %in% names(initial_data) && nrow(initial_data$calendar_dates) > 0L) {
            if ("exception_date" %in% names(initial_data$calendar_dates)) {
                initial_data$calendar_dates[, exception_type := exception_date]
            }
            services_actifs_cd <- initial_data$calendar_dates[
                date == fraicheur, .(service_id, exception_type)
            ]
            services_actifs <- services_actifs_cd |>
                dplyr::full_join(services_actifs, by = "service_id") |>
                dplyr::filter(is.na(exception_type) | exception_type == 1) |>
                dplyr::select(-exception_type)
        }
    }

    if (nrow(services_actifs) == 0) {
        cli::cli_alert_warning(
            "Dataset {.key {dataset_id}} : aucun service actif aujourd'hui, on passe."
        )
        result_extract <- rbind(
            result_extract, list(dataset_id = dataset_id, result = "no services")
        )
        next
    }

    tryCatch(
        {
            # --- Filtrage sur les trips et routes du jour ---
            services_actifs <- unique(data.table::as.data.table(services_actifs)[, .(service_id)])
            initial_data$trips <- initial_data$trips[services_actifs, on = "service_id"]
            initial_data$routes <- initial_data$routes[route_id %in% initial_data$trips$route_id]

            # --- Corrections colonnes manquantes ---
            if (nrow(initial_data$stops) != data.table::uniqueN(initial_data$stops$stop_id)) {
                initial_data$stops <- initial_data$stops[, head(.SD, 1), stop_id]
            }

            if (!"location_type" %in% colnames(initial_data$stops)) {
                initial_data$stops[, location_type := 0]
            }
            initial_data$stops[location_type == "", location_type := 0]

            if (!"route_short_name" %in% colnames(initial_data$routes)) {
                initial_data$routes[, route_short_name := route_long_name]
            } else if (!"route_long_name" %in% colnames(initial_data$routes)) {
                initial_data$routes[, route_long_name := route_short_name]
            }

            if (!"agency_id" %in% colnames(initial_data$agency)) {
                initial_data$agency[, agency_id := ""]
            }
            if (!"agency_id" %in% colnames(initial_data$routes)) {
                initial_data$routes[, agency_id := initial_data$agency$agency_id[1]]
            }
            if (!"agency_lang" %in% colnames(initial_data$agency)) {
                initial_data$agency[, agency_lang := NA_character_]
            }
            if (!"parent_station" %in% colnames(initial_data$stops)) {
                initial_data$stops[, parent_station := ""]
            }

            if (!"pickup_type" %in% colnames(initial_data$stop_times)) {
                initial_data$stop_times[, pickup_type := 0]
            }
            if (!"drop_off_type" %in% colnames(initial_data$stop_times)) {
                initial_data$stop_times[, drop_off_type := 0]
            }

            # --- Construction des tables de travail ---
            stations <- gtfstools::get_parent_station(initial_data)
            stations[parent_station == "", parent_station := stop_id]

            stops <- stations |>
                dplyr::left_join(
                    initial_data$stops[, .(stop_id, stop_name, stop_lat, stop_lon)],
                    by = c("parent_station" = "stop_id")
                )

            agency <- unique(initial_data$agency[, .(agency_id, agency_name, agency_lang)])

            cols_routes <- colnames(initial_data$routes)[
                colnames(initial_data$routes) %in% c(
                    "route_id", "route_type", "route_short_name", "route_long_name", "agency_id"
                )
            ]
            routes <- unique(initial_data$routes[, ..cols_routes])
            routes[, route_short_name := data.table::fifelse(
                route_short_name == "", route_long_name, route_short_name
            )]

            trips <- unique(initial_data$trips[, .(route_id, trip_id)])
            stop_times <- unique(
                initial_data$stop_times[
                    , .(trip_id, stop_id, stop_sequence, arrival_time, pickup_type, drop_off_type)
                ]
            )
            stop_times[is.na(pickup_type), pickup_type := 0]
            stop_times[is.na(drop_off_type), drop_off_type := 0]

            # --- Jointures stops x routes ---
            j1 <- merge(stops, stop_times, by = "stop_id", allow.cartesian = TRUE)
            j2 <- merge(j1, trips, by = "trip_id", allow.cartesian = TRUE)
            j3 <- agency[routes, on = "agency_id"][j2, on = "route_id"]

            # Transport à la demande
            j3[, ligne_ad := all(drop_off_type == 2), route_id]
            j3[, ligne_ad_partiel := any(drop_off_type == 2), route_id]
            j3[(ligne_ad), ligne_ad_partiel := FALSE]

            arret_route <- j3[!is.na(trip_id), head(.SD, 1), .(parent_station, route_id)][
                order(parent_station, route_id)
            ]

            # --- Fréquence PPM (7h-9h) ---
            passage_ppm <- j3[
                (pickup_type == 0 | drop_off_type == 0) &
                    data.table::between(lubridate::hour(lubridate::hms(arrival_time)), 7, 8),
                .N,
                .(stop_id, parent_station, route_id, route_type, stop_sequence)
            ] |>
                dplyr::slice_max(
                    order_by = N, n = 1, with_ties = FALSE,
                    by = c(parent_station, route_id, route_type)
                ) |>
                dplyr::select(-c(stop_id, stop_sequence)) |>
                dplyr::rename(freq_ppm_max = N)

            arret_route_freq <- passage_ppm[
                arret_route,
                on = .(parent_station, route_id, route_type)
            ]

            # --- Type de réseau ---
            arret_route_freq[, route_type := dplyr::case_match(
                as.character(route_type),
                c("3", "11", as.character(c(200:209, 700:716, 800))) ~ "bus",
                c("0", "5", as.character(900:906)) ~ "tramway",
                c("1", as.character(400:405)) ~ "métro",
                c("2", as.character(100:117)) ~ "train",
                c(
                    "4", "6", "7", "12",
                    as.character(c(1000, 1100, 1200, 1300:1307, 1400, 1500:1507, 1700:1702))
                ) ~ "autres",
                .default = "non renseigné"
            )]
            arret_route_freq[(ligne_ad), route_type := paste(route_type, "TAD")]

            # Rang priorité mode de transport
            arret_route_freq[, rang_route_type := 999L]
            arret_route_freq[route_type == "train", rang_route_type := 1L]
            arret_route_freq[route_type == "métro", rang_route_type := 2L]
            arret_route_freq[route_type == "tramway", rang_route_type := 3L]
            arret_route_freq[route_type %in% c("bus", "bus TAD"), rang_route_type := 4L]

            arret_route_freq <- arret_route_freq |>
                dplyr::select(-dplyr::any_of(
                    c(
                        "trip_id", "arrival_time", "stop_sequence",
                        "pickup_type", "drop_off_type", "ligne_ad"
                    )
                ))

            arret_route_freq[
                order(rang_route_type),
                ligne_princ := data.table::frank(rang_route_type, ties.method = "dense"),
                .(agency_id, agency_name, route_short_name)
            ]

            # --- Nettoyage noms d'arrêts ---
            stopwords_fr <- c(
                stopwords::stopwords("fr", source = "snowball"),
                "rer", "terminus", "hall", "depart", "arrivee"
            )
            stopwords_fr <- stopwords_fr[stopwords_fr != "est"]

            stop_names <- unique(arret_route_freq[, "stop_name"])
            stop_names[, stop_name_red := stop_name]
            stop_names[, stop_name_red := stringi::stri_trans_general(stop_name_red, "Latin-ASCII")]
            stop_names[, stop_name_red := stringr::str_to_lower(
                stringr::str_squish(
                    stringr::str_replace_all(
                        stop_name_red, c("[:digit:]" = " ", "[:punct:]" = " ", "\\W" = " ")
                    )
                )
            )]

            red_stop_names <- stop_names |>
                tidyr::separate_longer_delim(stop_name_red, " ") |>
                dplyr::filter(
                    !stop_name_red %in% stopwords_fr, !grepl("^\\w{1}$", stop_name_red)
                ) |>
                dplyr::mutate(stop_name_red = dplyr::case_match(
                    stop_name_red,
                    "saint" ~ "st", "sainte" ~ "ste", "route" ~ "rte",
                    "chemin" ~ "ch", "boulevard" ~ "bd", "avenue" ~ "av",
                    .default = stop_name_red
                )) |>
                tidyr::nest(stop_name_red = stop_name_red) |>
                dplyr::mutate(
                    stop_name_red = purrr::map_chr(
                        stop_name_red, \(x) paste0(x[[1]], collapse = " ")
                    )
                ) |>
                dplyr::right_join(arret_route_freq, by = "stop_name") |>
                data.table::as.data.table()

            red_stop_names[is.na(stop_name_red), stop_name_red := stop_name]

            arret_route_nom_red <- red_stop_names[
                ligne_princ == 1, head(.SD, 1), .(route_short_name, stop_name_red, parent_station)
            ]

            stations_routes <- arret_route_nom_red |>
                dplyr::select(-stop_id) |>
                dplyr::rename(stop_id = parent_station)

            # --- Table finale : une ligne par station x route ---
            processed_data_stops <- stations_routes |>
                dplyr::mutate(across(c("stop_name", "route_long_name"), stringr::str_to_title)) |>
                dplyr::mutate(across(c("stop_name", "route_long_name"), stringr::str_squish)) |>
                dplyr::mutate(
                    date_extraction = as.character(fraicheur),
                    dataset_id = dataset_id,
                    .before = 1
                ) |>
                dplyr::mutate(
                    latitude = as.numeric(stop_lat),
                    longitude = as.numeric(stop_lon),
                    .keep = "unused"
                ) |>
                dplyr::mutate(
                    stop_id_red = dplyr::coalesce(
                        stringr::str_extract(stop_id, "(\\w+)$"), stop_name_red
                    ),
                    .after = stop_id
                ) |>
                dplyr::mutate(
                    route_id_red = dplyr::coalesce(
                        stringr::str_extract(route_id, "(\\w+)$"), route_short_name
                    ),
                    .after = route_id
                ) |>
                dplyr::select(
                    dplyr::starts_with("stop_"), dplyr::starts_with("route_"), everything()
                ) |>
                dplyr::filter(!is.na(latitude) | !is.na(route_id)) |>
                dplyr::select(-ligne_princ) |>
                dplyr::mutate(
                    across(where(is.character), \(x) iconv(x, from = "", to = "UTF-8", sub = ""))
                )

            cli::cli_alert_info(
                "Dataset {.key {dataset_id}} ({i}/{n_datasets}) : {nrow(processed_data_stops)} arrêts traités."
            )

            all_stops_data <- rbind(all_stops_data, processed_data_stops, fill = TRUE)
            result_extract <- rbind(
                result_extract, list(dataset_id = dataset_id, result = "success")
            )
        },
        error = function(e) {
            cli::cli_alert_warning(
                "Dataset {.key {dataset_id}} : erreur de traitement ({conditionMessage(e)}), on passe."
            )
            result_extract <<- rbind(
                result_extract, list(dataset_id = dataset_id, result = "processing error")
            )
        }
    )

    suppressWarnings(
        rm(
            initial_data, agency, stops, stations, stop_times, trips, routes, j1, j2, j3,
            arret_route, arret_route_freq, red_stop_names, stations_routes, processed_data_stops
        )
    )
    gc()
}


### Consolidation des tables brutes GTFS

cli::cli_h1("Consolidation des tables GTFS brutes")

tables_gtfs <- c("stops", "routes", "trips", "stop_times", "agency", "calendar", "calendar_dates")
consolidated_dir <- file.path(data_dir, "transport.data.gouv.fr", "consolidated", fraicheur)
fs::dir_create(consolidated_dir)

for (tbl in tables_gtfs) {
    fichiers <- list.files(file.path(raw_dir, tbl), pattern = "\\.parquet$", full.names = TRUE)
    if (length(fichiers) == 0) {
        cli::cli_alert_warning("Table {.val {tbl}} : aucun fichier trouvé, on passe.")
        next
    }
    out_path_tbl <- file.path(consolidated_dir, glue("{tbl}.parquet"))
    arrow::open_dataset(fichiers) |>
        dplyr::collect() |>
        arrow::write_parquet(out_path_tbl)
    cli::cli_alert_success("Table {.val {tbl}} consolidée ({length(fichiers)} datasets).")
}

# Tableau des réseaux
networks_observed <- data.table::rbindlist(networks_list, fill = TRUE)

gtfs_datasets_info <- gtfs_datasets_info |>
    dplyr::left_join(networks_observed, by = "resources_id")

arrow::write_parquet(
    gtfs_datasets_info,
    file.path(
        data_dir, "transport.data.gouv.fr",
        glue("{gsub('-','',fraicheur)}_gtfs_datasets_info.parquet")
    )
)

### test
list_cols <- names(gtfs_datasets_info)[sapply(gtfs_datasets_info, is.list)]

for (col in list_cols) {
    cat("Test:", col, "\n")

    tmp <- gtfs_datasets_info |>
        dplyr::select(resources_id, dplyr::all_of(col))

    tryCatch(
        {
            arrow::write_parquet(tmp, tempfile(fileext = ".parquet"))
            cat("OK\n")
        },
        error = function(e) {
            cat("ERREUR :", conditionMessage(e), "\n")
        }
    )
}
### fin test


### Sauvegarde des résultats

cli::cli_h1("Sauvegarde")

# Rapport d'extraction
arrow::write_parquet(
    result_extract,
    file.path(
        data_dir, "transport.data.gouv.fr",
        glue("{gsub('-','',fraicheur)}_resultats_extraction.parquet")
    )
)

# Nettoyages finaux sur la table compilée
all_stops_data <- all_stops_data[!is.na(latitude) | is.na(route_id)]
all_stops_data[, agency_lang := toupper(agency_lang)]
all_stops_data[agency_lang == "", agency_lang := NA_character_]
all_stops_data[agency_lang == "FR-FR", agency_lang := "FR"]
all_stops_data[is.na(freq_ppm_max), freq_ppm_max := 0]
all_stops_data <- unique(all_stops_data)

nb_nr <- all_stops_data[route_type == "non renseigné", .N]
if (nb_nr < nrow(all_stops_data) * 0.001) {
    all_stops_data[route_type == "non renseigné", route_type := "autres"]
}

# Déduplication
all_stops_data <- all_stops_data[
    order(-freq_ppm_max),
    head(.SD, 1), .(stop_name_red, route_short_name, route_type, latitude, longitude)
]
all_stops_data[, `:=`(arrond_lat = round(latitude, 3), arrond_lon = round(longitude, 3))]
all_stops_data <- all_stops_data[
    order(-freq_ppm_max),
    head(.SD, 1), .(stop_name_red, route_short_name, route_type, arrond_lat, arrond_lon)
]
all_stops_data <- all_stops_data[
    order(-freq_ppm_max),
    head(.SD, 1), .(stop_id, stop_name_red, route_id, agency_id)
]

# Sauvegarde parquet
out_path <- file.path(data_dir, glue("all_stops_data_{format(fraicheur, '%Y%m%d')}.parquet"))
arrow::write_parquet(all_stops_data, out_path)

cli::cli_alert_success("Terminé ! {nrow(all_stops_data)} arrêts compilés.")
cli::cli_alert_success("Fichier sauvegardé : {.path {out_path}}")
cli::cli_alert_info(
    "Rapport d'extraction : {sum(result_extract$result == 'success')}/{nrow(result_extract)} datasets traités avec succès."
)
