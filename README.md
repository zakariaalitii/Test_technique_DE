# Chicago Taxi Trips — Pipeline Data Engineering (Medallion)

> **Test technique Data Engineer** — pipeline local reproductible, orchestré avec Apache Airflow, transformé avec PySpark et déployé avec Docker Compose.

## 1. Objectif et périmètre

Construire un pipeline de bout en bout à partir des données publiques **Chicago Taxi Trips** : ingestion CSV via l'API SODA, conservation des données brutes, nettoyage et contrôles qualité, agrégations métier, exposition SQL et visualisation BI.

**Périmètre de démonstration :** du **1er janvier 2023 inclus au 1er février 2023 exclu**, avec un plafond de **100 000 lignes**, extraites par pages de **10 000**. Le plafond permet une exécution raisonnable sur un ordinateur local ; il ne correspond pas nécessairement à l'intégralité des trajets de cette période.

## 2. Prérequis

- **Docker Desktop** avec Docker Compose v2, démarré et configuré pour les conteneurs Linux ; Windows 11 / PowerShell ou un environnement Linux compatible.
- Ressources conseillées : **8 Go de RAM disponibles** (davantage si possible), plusieurs Go d'espace disque et accès Internet pour les images Docker, les dépendances Spark et l'API Chicago.
- Ports hôte disponibles : **8080** (Airflow), **9000** (API S3), **9001** (console MinIO/Silo), **5433** (PostgreSQL Warehouse), **3000** (Metabase).
- Le fichier `.env` est créé à partir de `.env.example`. Ne pas publier de mots de passe réels dans Git.

Vérifier l'installation :

```powershell
docker --version
docker compose version
docker info
```

## 3. Démarrage pas à pas (PowerShell)

À exécuter **à la racine du projet**, là où se trouve `docker-compose.yml` :

```powershell
# 1. Configurer les variables d'environnement
Copy-Item .env.example .env
notepad .env

# 2. Préparer les logs Airflow
New-Item -ItemType Directory -Force logs | Out-Null

# 3. Construire et démarrer les services
docker compose up -d --build

# 4. Vérifier l'état des conteneurs
docker compose ps -a

# 5. Vérifier l'import du DAG
docker compose exec airflow-scheduler airflow dags list-import-errors
```

Adapter dans `.env` les identifiants de MinIO/Silo et de PostgreSQL. Les paramètres de la source sont notamment :

```dotenv
TAXI_START=2023-01-01T00:00:00
TAXI_END=2023-02-01T00:00:00
TAXI_MAX_ROWS=100000
TAXI_PAGE_SIZE=10000
S3_BUCKET=taxi-lake
S3_ENDPOINT=http://minio:9000
WAREHOUSE_HOST=warehouse
WAREHOUSE_PORT=5432
WAREHOUSE_DB=taxi
WAREHOUSE_USER=taxi
```

**Attention :** `WAREHOUSE_HOST=warehouse` et `WAREHOUSE_PORT=5432` correspondent à la communication **entre conteneurs**. Depuis Windows, la connexion au warehouse utilise **`localhost:5433`**. Dans Metabase, utiliser **`warehouse:5432`**, jamais `localhost:5433`.

### 3.1. Initialiser un ancien volume PostgreSQL si nécessaire

Lors de la première création d'un volume vide, PostgreSQL exécute automatiquement les scripts dans `sql/init/`. Si le volume existait avant l'ajout de ces scripts, les schémas peuvent être absents. Vérifier :

```powershell
docker compose exec warehouse psql -U taxi -d taxi -c "\dn"
```

Si seuls les objets `public` apparaissent, appliquer le script **sans supprimer les volumes** :

```powershell
Get-Content -Raw .\sql\init\01_warehouse.sql | docker compose exec -T warehouse psql -v ON_ERROR_STOP=1 -U taxi -d taxi
```

Cette commande est prévue pour un warehouse **non initialisé** ; ne pas la rejouer aveuglément sur un schéma déjà créé. Si vous avez modifié `WAREHOUSE_USER` ou `WAREHOUSE_DB`, remplacer `taxi` dans les commandes.

### 3.2. Exécuter le pipeline

1. Ouvrir **Airflow : http://localhost:8080**.
2. Se connecter avec le compte local configuré dans `docker-compose.yml` (configuration de démonstration : `admin` / `admin`).
3. Rechercher le DAG **`test_technique_data_engineer_ma`**.
4. Activer le DAG si nécessaire, puis cliquer sur **Trigger DAG**.
5. Dans **Graph**, attendre que les **11 tâches** deviennent vertes.

Autre possibilité, depuis PowerShell :

```powershell
docker compose exec airflow-scheduler airflow dags trigger test_technique_data_engineer_ma
```

**Interfaces :**

| Service | Accès local | Utilité |
|---|---|---|
| Airflow | http://localhost:8080 | DAG, tâches, logs, relances |
| MinIO/Silo | http://localhost:9001 | Exploration du bucket `taxi-lake` |
| Metabase | http://localhost:3000 | Exploration SQL et tableaux de bord |
| PostgreSQL Warehouse | `localhost:5433` | Connexion DBeaver/pgAdmin |

Pour connecter Metabase au warehouse : type **PostgreSQL**, hôte `warehouse`, port `5432`, base `taxi`, utilisateur et mot de passe définis dans `.env`.

### 3.3. Vérifier les résultats

```powershell
# Santé et logs
docker compose ps -a
docker compose logs airflow-scheduler --tail=100

# Nombre de trajets publiés dans la table de faits
docker compose exec warehouse psql -U taxi -d taxi -c "SELECT COUNT(*) FROM gold.fact_trips;"

# Liste des schémas
docker compose exec warehouse psql -U taxi -d taxi -c "\dn"

# Tests automatisés inclus dans le projet
docker compose exec airflow-scheduler bash -lc "cd /opt/airflow/project && pytest -q tests"
```

Pour arrêter les services **en conservant les données** : `docker compose down`. **Ne pas utiliser `docker compose down -v`** sauf si la suppression définitive des volumes est voulue.

## 4. Architecture globale

![Architecture globale du projet](docs/img/archi.png)

```text
Chicago Taxi Trips (API SODA / CSV)
             |
             v
      Python + Requests + Boto3  <---- Airflow (orchestration)
             |
             v
  MinIO/Silo — BRONZE : CSV brut + Parquet dérivé
             |
             v
       PySpark + DQ1 (Great Expectations)
             |-----------------> MinIO/Silo — REJECTS : Parquet + motif
             v
  MinIO/Silo — SILVER : Parquet nettoyé et dédupliqué
             |
             v
       DQ2 + PySpark (agrégations)
             |
             v
  MinIO/Silo — GOLD : Parquet agrégé (KPIs)
             |
             v
          DQ3 (validation)
             |
             v
  PostgreSQL — couche de service BI
    |        |               |
  Silver   Schéma étoile    Tables/vues KPI
             |
             v
          Metabase

Transverse : PostgreSQL monitoring.pipeline_runs + data_quality.check_results
Exécution locale : Docker Compose
```

**Point architectural important :** les couches **Bronze, Silver et Gold de référence sont toutes matérialisées dans MinIO/Silo**. PostgreSQL est une **copie d'exposition BI**, et non le stockage principal des couches médaillon. Le schéma d'architecture illustratif peut présenter Power BI ; **l'outil BI réellement déployé est Metabase**.

## 5. Choix techniques et arbitrages

| Besoin | Technologie retenue | Alternatives | Justification / compromis |
|---|---|---|---|
| Ingestion | Python (`requests`, `boto3`) | NiFi, Airbyte | API REST paginée simple ; code léger, versionnable et intégré au DAG. NiFi serait pertinent pour de nombreuses sources hétérogènes. |
| Orchestration | Apache Airflow | Dagster, Prefect | DAG explicite, reprises et logs, outil imposé ou accepté par le sujet. |
| Processing | Apache Spark / PySpark | Pandas, Polars, DuckDB | Exigence du test ; transformations distribuables malgré une exécution locale. |
| Stockage médaillon | MinIO/Silo compatible S3 + Parquet | S3 cloud, HDFS, PostgreSQL seul | Bronze/Silver/Gold sur stockage objet, sans service cloud payant ; Parquet compressé et exploitable par Spark. |
| Qualité | Great Expectations + contrôles Spark/SQL | Soda, Deequ | Validation à plusieurs étapes, rejet explicite et traçabilité. |
| Couche BI | PostgreSQL | Trino, DuckDB, ClickHouse | SQL simple et intégration immédiate avec Metabase ; duplication maîtrisée des données du lac. |
| Modélisation | Schéma en étoile | Tables plates, Data Vault | Mesures centralisées et dimensions réutilisables pour les analyses métier. |
| Visualisation | Metabase | Power BI, Superset | Open source, conteneur local, connexion PostgreSQL. |
| Déploiement | Docker Compose | Kubernetes | Reproductibilité sur un seul poste avec une commande, sans infrastructure externe. |

**Arbitrage principal :** privilégier une architecture conforme au sujet (stockage objet pour **les trois couches**) plutôt qu'une variante Silver/Gold directement dans PostgreSQL. PostgreSQL reste utile pour servir les KPI à Metabase.

## 6. Pipeline et qualité des données

| Ordre | Tâche Airflow | Fonction |
|---:|---|---|
| 1 | `extract_to_bronze` | Télécharger les pages CSV de l'API et conserver les données brutes. |
| 2 | `bronze_csv_to_parquet` | Produire une version Parquet optimisée sans remplacer le CSV brut. |
| 3 | `dq1_bronze` | Vérifier schéma et règles de validité ; isoler les rejets. |
| 4 | `write_silver_minio` | Matérialiser les trajets nettoyés dans Silver/MinIO. |
| 5 | `dq2_silver` | Vérifier notamment les valeurs obligatoires, l'unicité et les règles métier. |
| 6 | `build_gold_minio` | Calculer les agrégats métier avec PySpark et écrire Gold/MinIO. |
| 7 | `dq3_gold_minio` | Contrôler la cohérence des agrégats du lac. |
| 8 | `publish_silver_postgres` | Charger une copie de Silver dans PostgreSQL pour les usages SQL. |
| 9 | `publish_star_schema` | Construire la table de faits et les dimensions. |
| 10 | `publish_gold_postgres` | Publier les KPI calculés dans la couche BI PostgreSQL. |
| 11 | `validate_bi_postgres` | Réconcilier les résultats exposés en BI. |

### Organisation du bucket

```text
taxi-lake/
├── bronze/           # CSV d'origine, conservés
├── bronze_parquet/   # Conversion technique du Bronze
├── validated/        # Données validées après DQ1
├── rejects/          # Enregistrements invalides, motifs et métadonnées
├── silver/           # Trajets nettoyés au format Parquet
└── gold/             # Agrégations Parquet
    └── chicago_taxi/start=2023-01-01/end=2023-02-01/
        ├── daily_revenue/
        ├── trip_statistics/
        ├── pickup_zones/
        └── payment_methods/
```

Les chemins sont organisés par période (`start=.../end=...`). Les contrôles sont enregistrés dans `data_quality.check_results` ; les métriques d'exécution dans `monitoring.pipeline_runs`.

## 7. Modélisation et indicateurs métier

![Schéma en étoile — table de faits et dimensions](docs/img/data_modling.png)

**Grain de `gold.fact_trips` : un enregistrement par trajet unique (`trip_id`).** Dimensions : `dim_date`, `dim_company`, `dim_payment_type`, `dim_location`. La dimension `dim_location` joue **deux rôles** via `pickup_location_key` et `dropoff_location_key`.

Les KPI exposés comprennent :

- **Daily revenue** : nombre de trajets, somme de `trip_total`, montant moyen par trajet ;
- **Trip statistics** : durée et distance moyennes des trajets ;
- **Pickup zones** : nombre de trajets et montant total par zone de prise en charge ;
- **Payment methods** : répartition des trajets et montants par mode de paiement.

Le « revenue » correspond ici à la **somme des montants facturés (`trip_total`)**, indicateur analytique brut et non chiffre d'affaires net comptable.

## 8. Démonstration — captures d'écran

### 8.1. Orchestration Airflow

![DAG Airflow : 11 tâches réussies](docs/img/airflow.png)

### 8.2. Structure du lac de données

![MinIO : Bronze, Silver, Gold et rejets](docs/img/datalake.png)

### 8.3. Silver dans MinIO

![MinIO : Silver Parquet](docs/img/MinIO Silver.png)

### 8.4. Gold dans MinIO

![MinIO : Gold Parquet](docs/img/MinIO Gold.png)

### 8.5. PostgreSQL : couche BI

![PostgreSQL : schémas](docs/img/postgresql.png)

![PostgreSQL : tables et KPI Gold](docs/img/gold.png)

### 8.6. Contrôles de qualité

![Résultats des contrôles DQ1/DQ2/DQ3](docs/img/dq.png)

### 8.7. Monitoring

![Suivi des tâches et des volumes](docs/img/monitoring.png)

### 8.8. Visualisation Metabase

![Répartition des trajets et des montants par paiement](docs/img/data_vis.png)

## 9. Résultats observés (exécution du 25/10/2026)

| Indicateur | Valeur observée |
|---|---:|
| Lignes ingérées | **100 000** |
| Lignes validées | **99 870** |
| Lignes rejetées | **130** |
| Taux de lignes acceptées | **99,87 %** |
| Tâches du DAG en succès | **11 / 11** |

Ces valeurs correspondent à **la démonstration présentée**, et ne constituent pas une garantie de résultats identiques si le jeu de données public évolue.

## 10. Limites et améliorations avec plus de temps

1. **Fiabiliser l'ingestion incrémentale** : curseur stable, reprise après interruption, gestion avancée des limites de débit de l'API et snapshots source.
2. **Transactions et gouvernance du lac** : introduire Apache Iceberg ou Delta Lake pour les mises à jour atomiques, le versionnement et les évolutions de schéma.
3. **Qualité et observabilité** : tests automatisés plus étendus, seuils d'alerte, notifications, tableaux de bord de fraîcheur, SLA et traçabilité complète.
4. **Industrialiser le déploiement** : CI/CD, gestion des secrets, images versionnées, environnements dev/test/prod et exécution Spark sur cluster.
5. **Optimiser les performances** : partitionnement et compactage Parquet, chargements incrémentaux, index PostgreSQL, benchmark sur plusieurs millions de lignes.
6. **Améliorer le BI** : dashboard Metabase complet (revenu quotidien, durée moyenne, top zones), documentation métier et tests de réconciliation.

## 11. Structure principale du dépôt

```text
.
├── dags/                    # DAG Airflow
├── ingestion/               # API SODA -> CSV Bronze
├── bronze/                  # Conversion CSV -> Parquet
├── silver/                  # Silver MinIO et publication SQL
├── gold/                    # Gold MinIO, étoile et publication BI
├── data_quality/            # DQ1, DQ2, DQ3
├── spark/                   # Transformations et utilitaires PySpark
├── monitoring/              # Métriques de pipeline
├── sql/init/                # Initialisation PostgreSQL
├── tests/                   # Tests
├── config/                  # Configuration du pipeline
├── docs/img/                # Captures et diagrammes du rapport
├── docker-compose.yml
├── Dockerfile.airflow
├── .env.example
└── README.md
```

**Note :** les captures démontrent une exécution réussie sur la machine de développement. La reproductibilité complète doit être vérifiée par une exécution de bout en bout sur un environnement Docker propre avant livraison.
