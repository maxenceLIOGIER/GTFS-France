# GTFS-France

Récupération de l'ensemble des GTFS disponibles en France, dans le but de calculer, à terme, des indicateurs d'accessibilité aux transports en commun (TC).

## Sommaire

- [Objectif](#objectif)
- [Principe de fonctionnement](#principe-de-fonctionnement)
- [Structure du dépôt](#structure-du-dépôt)
- [Utilisation](#utilisation)
- [Données produites](#données-produites)
- [Limites connues](#limites-connues)

## Objectif

Ce projet récupère automatiquement l'ensemble des jeux de données GTFS référencés sur [transport.data.gouv.fr](https://transport.data.gouv.fr), les nettoie, les standardise et les consolide dans un format unique exploitable à l'échelle nationale. L'objectif final est de pouvoir en dériver des indicateurs d'accessibilité aux transports en commun (fréquence de desserte, couverture géographique, etc.).

## Principe de fonctionnement

Le script (`traitements_gtfs.py`) exécute les étapes suivantes :
1. **Récupération des métadonnées** : interrogation de l'API `transport.data.gouv.fr/api/datasets` pour lister tous les jeux de données au format GTFS disponibles.
2. **Téléchargement** de chaque GTFS (`gtfs.zip`)
3. **Extraction et lecture** : extraction des fichiers `.txt` de chaque GTFS et lecture en DataFrames polars, avec nettoyage des noms de colonnes et validation des fichiers/colonnes obligatoires (`stops`, `routes`, `trips`, `stop_times`).
4. **Traitement par dataset** :
   - préfixage des identifiants (`stop_id`, `route_id`, `trip_id`...) par l'identifiant du dataset, pour éviter les collisions entre réseaux ;
   - filtrage sur les services actifs à la date de référence (`FRAICHEUR`) ;
   - rattachement des arrêts à leur station parente ;
   - calcul d'une fréquence de passage en heure de pointe matinale (7h-9h) par arrêt/ligne ;
   - catégorisation du mode de transport ;
   - nettoyage des noms d'arrêts (suppression des stopwords, normalisation des abréviations).
5. **Sauvegarde** des tables brutes par dataset au format Parquet (`data/transport.data.gouv.fr/raw_tables/<date>/`), puis **consolidation** de l'ensemble des GTFS traités.
6. **Compilation finale** : déduplication des arrêts (par nom/coordonnées/ligne) et export d'un fichier unique `data/all_stops_data_<date>.parquet`.

Un second script, `rapport_reseaux.py`, produit ensuite un rapport des réseaux en faisant deux choses :
1. **Recensement de l'ensemble des réseaux existants** référencés sur le PAN (Point d'Accès National), avec des indicateurs par réseau (ex : dates de début/fin observées, réseau hors période de validité...) ;
2. **Rapprochement entre GTFS individuels et agrégats régionaux** (basé sur le nom d'agence), pour identifier quels sont les réseaux couverts par un agrégat

## Structure du dépôt

```
GTFS-France/
├── src/
│   ├── config.py            # Paramètres du pipeline (chemins, dates, mappings, colonnes requises)
│   ├── utils_gtfs.py        # Fonctions utilitaires (API, téléchargement, lecture GTFS, nettoyage)
│   ├── traitements_gtfs.py  # Script principal : télécharge, traite et consolide tous les GTFS
│   ├── rapport_reseaux.py   # Rapprochement GTFS individuels <-> agrégats régionaux
│   └── _old/                # Anciens scripts (R et Python), non maintenus
└── README.md
```

> `utils_gtfs.py` reprend la logique du package R `territoRy` utilisé par la DRIEAT pour le tableau de bord des mobilités durables et dont ce travail s'inspire.

## Utilisation

Paramètres modifiables dans `config.py` :
| Paramètre | Rôle |
|---|---|
| `FRAICHEUR` | Date de référence utilisée pour filtrer les services actifs (par défaut aujourd'hui mais peut être modifiée) |
| `REDOWNLOAD` | Si `True`, télécharge les GTFS même s'ils sont déjà présents localement |
| `DATA_DIR` | Répertoire de sortie des données (variable d'environnement `DATA_DIR`, sinon `./data`) |

Le premier script à exécuter est `traitements_gtfs.py` qui permet de télécharger les données. Ce n'est qu'ensuite que l'on peut lancer `rapport_reseaux.py` pour construire le tableau des réseaux.

## Données produites

Pour une date :

| Fichier | Contenu |
|---|---|
| `data/transport.data.gouv.fr/<date>/<resources_id>/gtfs.zip` | GTFS brut téléchargé par dataset |
| `data/transport.data.gouv.fr/raw_tables/<date>/<table>/<resources_id>.parquet` | Tables GTFS brutes, par dataset, préfixées et validées |
| `data/transport.data.gouv.fr/consolidated/<date>/<table>.parquet` | Tables GTFS consolidées tous datasets confondus |
| `data/all_stops_data_<date>.parquet` | Table finale : un arrêt x ligne, avec fréquence, mode de transport, coordonnées |
| `data/transport.data.gouv.fr/<date>_resultats_extraction.parquet` | Statut de traitement de chaque dataset (succès, échec, raison) |
| `data/transport.data.gouv.fr/<date>_gtfs_datasets_info.parquet` | Rapport de rapprochement GTFS individuels / agrégats régionaux |

## Limites connues

- Le rapprochement GTFS individuel / agrégat régional (`rapport_reseaux.py`) se base sur le nom d'agence, avec une liste manuelle et non exhaustive d'agences trop génériques à exclure (Keolis, Transdev, SNCF...). Les métadonnées du PAN (Point d'Accès National) auraient pu être utilisées mais ne sont pas standardisées selon les agrégats.
- La question des indicateurs d'accessibilité n'a pas encore été réellement traitée et n'est donc pas développée.