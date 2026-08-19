# Documentation — `gtfs_datasets_info`

## 1. Objectif

`gtfs_datasets_info` est le rapport final produit par `rapport_reseaux.py`. Il liste l'ensemble des jeux de données GTFS référencés sur [transport.data.gouv.fr](https://transport.data.gouv.fr),
en croisant :

- les **métadonnées déclaratives** de l'API transport.data.gouv.fr (titre, éditeur,
  zone géographique couverte, offres de transport, etc.) ;
- les **métadonnées observées** directement dans les fichiers GTFS téléchargés
  (agences, dates de validité du calendrier) ;
- un **rapprochement automatique** entre chaque GTFS individuel et l'agrégat régional
  qui le contient éventuellement (ex. un réseau urbain intégré dans l'agrégat GTFS de
  sa région), basé sur les identifiants d'offres de transport de l'API (`offer_ids`).


## 2. Granularité

Une ligne = **une ressource GTFS** (`resource_transport_id`), c'est-à-dire un fichier
GTFS précis. Un même jeu de données (`dataset_id`) peut contenir plusieurs ressources.


## 3. Dictionnaire des champs

> **Colonne « Source »** : `API` = métadonnée déclarative de transport.data.gouv.fr. > `GTFS` = valeur recalculée à partir du fichier GTFS réellement
> téléchargé et parsé. `Pipeline` = valeur générée par le script lui-même.

| # | Colonne | Type | Source | Description |
|---|---|---|---|---|
| 1 | `dataset_id` | texte | API | Identifiant data.gouv.fr du **jeu de données** (peut regrouper plusieurs ressources GTFS). |
| 2 | `resource_transport_id` | entier | API | Identifiant interne transport.data.gouv.fr de la **ressource GTFS** (le fichier lui-même). |
| 3 | `resource_datagouv_id` | texte | API | Identifiant data.gouv.fr de la ressource GTFS. |
| 4 | `dataset_title` | texte | API | Titre du jeu de données tel qu'affiché sur transport.data.gouv.fr. |
| 5 | `resource_title` | texte | API | Titre de la ressource GTFS. |
| 6 | `page_url` | texte (url) | API | Page du jeu de données sur transport.data.gouv.fr. |
| 7 | `resource_url` | texte (url) | API | URL de téléchargement du fichier GTFS (zip), hébergée par transport.data.gouv.fr. |
| 8 | `resource_original_url` | texte (url) | API | URL de téléchargement d'origine, chez l'émetteur des données (à utiliser si `resource_url` échoue). |
| 9 | `resource_page_url` | texte (url) | API | Page dédiée à la ressource sur transport.data.gouv.fr. |
| 10 | `publisher_name` | texte | API | Nom de l'organisme éditeur du jeu de données (AOM, région, opérateur...). |
| 11 | `resource_community_resource_publisher` | texte | API | Si la ressource a été ajoutée par un tiers (et non par l'éditeur officiel), nom de ce contributeur communautaire. Vide sinon. |
| 12 | `resource_features` | liste de texte | API | Fonctionnalités GTFS détectées par la plateforme (ex. `tarification`, `couleur de ligne`...). |
| 13 | `resource_modes` | liste de texte | API | Modes de transport détectés dans le GTFS (ex. `bus`, `tram`...), déduits par transport.data.gouv.fr. |
| 14 | `stops_count` | entier | API | Nombre d'arrêts (`stops.txt`) déclaré par la plateforme pour ce GTFS. |
| 15 | `categorie_geo` | texte | API | Type de zone géographique couverte (`commune`, `epci`, `departement`, `region`, `pays`), déduit de `covered_area`. Sert de filtre dans le rapprochement agrégat (§5). |
| 16 | `covered_area` | liste de structures | API | Détail brut de la ou des zones géographiques couvertes par le jeu de données, tel que renvoyé par l'API. |
| 17 | `is_scolaire` | booléen | API | `True` si le jeu de données est tagué comme transport scolaire (`sub_types` contient `school`). |
| 18 | `is_saisonnier` | booléen | API | `True` si le jeu de données est tagué comme saisonnier (`sub_types` contient `seasonal`). |
| 19 | `is_agregat` | booléen | API | `True` si le jeu de données est un **agrégat régional** (tag `agrégat_region`). |
| 20 | `offer_ids` | liste d'entiers | API | Identifiants des offres de transport (réseaux) couvertes par le jeu de données, tels que renvoyés par l'API (`identifiant_offre`). |
| 21 | `offer_names` | liste de texte | API | Noms commerciaux correspondant à `offer_ids` (ex. `Ilévia`, `Star`...), pour lecture humaine. |
| 22 | `legal_owners` | liste de structures | API | Détail des propriétaires légaux des données, tel que renvoyé par l'API. |
| 23 | `created_at` | date (`DD/MM/YYYY`) | API | Date de création du jeu de données sur la plateforme. |
| 24 | `updated` | date/heure (ISO 8601) | API | Date de dernière mise à jour du jeu de données sur la plateforme. |
| 25 | `resource_extract_date` | date (`DD/MM/YYYY`) | Pipeline | Date de fraîcheur (`FRAICHEUR`, cf. `config.py`) à laquelle le pipeline a tourné — permet de dater chaque export et de suivre l'évolution d'un GTFS dans le temps entre deux runs. |
| 26 | `agency_name` | texte | GTFS | Nom(s) d'agence (`agency.txt`, champ `agency_name`) observé(s) directement dans le fichier téléchargé. |
| 27 | `start_date` | date (`DD/MM/YYYY`) | GTFS | Première date de validité du service, calculée à partir de `calendar.txt` et `calendar_dates.txt` du GTFS. |
| 28 | `end_date` | date (`JJ/MM/YYYY`) | GTFS | Dernière date de validité du service, même calcul que `start_date`. |
| 29 | `service_dates_valid` | booléen | GTFS | `True` si la date de fraîcheur (`FRAICHEUR`) est bien comprise entre `start_date` et `end_date` (le GTFS est donc à jour) ; `False` si la date de fraîcheur est en dehors de cette période (GTFS périmé ou pas encore actif). |
| 30 | `agregat_resources_id` | entier | Pipeline | `resource_transport_id` de l'agrégat régional dans lequel ce GTFS est intégré, s'il existe (cf. §4) ; `null` si ce GTFS est lui-même un agrégat (`is_agregat = True`), ou si aucun agrégat correspondant n'a été trouvé. |


## 4. Rapprochement GTFS individuel ↔ agrégat régional

Certaines régions publient, en plus des GTFS de chaque réseau de transport, un GTFS
« agrégat » qui les compile tous. Pour associer un GTFS individuel à l'agrégat qui le
contient (utile pour éviter de traiter deux fois les mêmes données, ou pour choisir
la source à privilégier), le pipeline :

1. repère tous les jeux de données `is_agregat = True` et construit, pour chacun,
   l'ensemble de ses `offer_ids` ;
2. pour chaque GTFS individuel, cherche un agrégat dont les `offer_ids` recoupent les
   siens, en écartant les agrégats dont la couverture géographique
   (`categorie_geo`) serait plus étroite que celle du GTFS individuel (un agrégat
   régional ne peut contenir un GTFS couvrant tout le pays) ;
3. le premier agrégat correspondant trouvé est enregistré dans
   `agregat_resources_id`.


## 5. Colonnes imbriquées et export CSV

Les colonnes suivantes contiennent des listes ou des structures (JSON) dans le
parquet : `resource_features`, `resource_modes`, `covered_area`, `offer_ids`,
`offer_names`, `legal_owners`. Le format CSV ne supportant pas les champs imbriqués,
elles sont aplaties à l'export :

- liste de valeurs simples (ex. `offer_ids`) → valeurs jointes par une virgule ;
- liste de structures ou structure simple (ex. `covered_area`, `legal_owners`) →
  encodées en JSON dans une seule cellule texte.
