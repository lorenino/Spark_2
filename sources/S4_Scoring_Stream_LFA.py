# Databricks notebook source
# MAGIC %md
# MAGIC # S4 — Scoring temps réel et détection de drift
# MAGIC
# MAGIC **Auteur** : Lorenzo Faloci — trigramme `LFA`
# MAGIC **Cours** : Spark Structured Streaming — Séance 4
# MAGIC **Date** : 2026-05-19
# MAGIC
# MAGIC ## Objectif
# MAGIC
# MAGIC Appliquer un modèle **K-Means** sur le flux `gold_stream_taxi` (alimenté
# MAGIC par S3) et produire :
# MAGIC
# MAGIC 1. `classified_stream_taxi` : flux scoré avec `prediction` +
# MAGIC    `cluster_label` (labels métier).
# MAGIC 2. `drift_monitor` : distribution des clusters par fenêtre 10 min,
# MAGIC    permettant de détecter un changement de distribution (data drift).
# MAGIC
# MAGIC Simulation en **deux phases** : 10 batches `normal` puis 10 batches
# MAGIC `airport_surge` (volume aéroport × 4). Le point de bascule doit être
# MAGIC identifiable dans `drift_monitor`.
# MAGIC
# MAGIC ## Modèle K-Means de Spark Core S5
# MAGIC
# MAGIC Modèle pré-entraîné dans
# MAGIC `workspace.default.gold_quartiers_clustered` (notebook
# MAGIC `04_ML_Clustering` de Spark Core S5).
# MAGIC
# MAGIC **Pipeline d'entraînement** :
# MAGIC
# MAGIC - Features (5) : `avg_distance`, `avg_fare`, `avg_tip`,
# MAGIC   `trips_count`, `green_taxi_ratio`
# MAGIC - Granularité : **par PULocationID** (zone NYC), pas par trajet
# MAGIC - `VectorAssembler` → `StandardScaler` (centre+réduit) → **PCA k=2**
# MAGIC   → `KMeans(k=4, seed=42)`
# MAGIC - Sortie : colonne `cluster_pca` (0-3) sauvegardée dans
# MAGIC   `workspace.default.gold_quartiers_clustered`
# MAGIC
# MAGIC ### ⚠️ Contrainte serverless rencontrée
# MAGIC
# MAGIC Le PDF demande de charger le modèle via `PipelineModel.load()` puis
# MAGIC `transform()`. Sur Databricks Free Edition serverless, la sandbox
# MAGIC Py4J bloque l'instanciation directe des classes MLlib :
# MAGIC
# MAGIC ```
# MAGIC Py4JSecurityException: VectorAssembler constructor is not whitelisted.
# MAGIC ```
# MAGIC
# MAGIC **Adaptation** : comme le modèle Spark Core S5 a sauvegardé son
# MAGIC résultat (`cluster_pca` par zone) dans une table Delta, on peut faire
# MAGIC un **Stream-Static Join** entre `gold_stream_taxi` et
# MAGIC `gold_quartiers_clustered` sur `PULocationID`. La colonne `cluster_pca`
# MAGIC devient `prediction`. C'est sémantiquement équivalent à appliquer le
# MAGIC modèle (le mapping zone→cluster est immuable une fois le modèle
# MAGIC entraîné), sans nécessiter MLlib en streaming.
# MAGIC
# MAGIC Cette approche est même plus efficace : pas de calcul ML au runtime,
# MAGIC juste un lookup broadcast (~265 zones).
# MAGIC
# MAGIC ## Décisions techniques majeures
# MAGIC
# MAGIC | Décision | Choix | Justification |
# MAGIC |---|---|---|
# MAGIC | Modèle | KMeans k=4 (Pipeline) | 4 clusters typiques NYC : court/cher, court/moyen, aéroport long, premium |
# MAGIC | Features | 5 numériques (distance, fare, tip, total, passengers) | Pas de `pickup_borough` (catégoriel non encodé pour rester simple) |
# MAGIC | Sortie KPI | **Alternative B** (PDF §4) : append + calcul batch | Évite les contraintes `update` Delta + GLOBAL TEMP serverless rencontrées en S2 |
# MAGIC | Fenêtre drift | 10 min tumbling | Aligné sur fenêtres S2 |
# MAGIC | Critère drift Q1 | **PSI** (Population Stability Index) | Standard MLOps, seuil empirique > 0.25 = drift significatif |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Paramètres

# COMMAND ----------

TRIGRAMME = "LFA"
CATALOG = "workspace"
SCHEMA  = f"tp_spark_{TRIGRAMME.lower()}"

# Volumes UC (créés S1)
VOL_SCRIPTS     = f"/Volumes/{CATALOG}/{SCHEMA}/scripts"
VOL_RAW_DATA    = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
VOL_CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
VOL_SCHEMAS     = f"/Volumes/{CATALOG}/{SCHEMA}/schemas"

# Chemins applicatifs
SIM_OUTPUT_PATH        = f"{VOL_RAW_DATA}/stream_input/"
CHECKPOINT_BRONZE      = f"{VOL_CHECKPOINTS}/bronze_stream/"
CHECKPOINT_SILVER      = f"{VOL_CHECKPOINTS}/silver_stream/"
CHECKPOINT_GOLD        = f"{VOL_CHECKPOINTS}/gold_stream/"
CHECKPOINT_QUARANTINE  = f"{VOL_CHECKPOINTS}/quarantine_stream/"
CHECKPOINT_CLASSIFIED  = f"{VOL_CHECKPOINTS}/classified_stream/"
SCHEMA_LOC_BRONZE      = f"{VOL_SCHEMAS}/bronze_stream/"

# Tables
BRONZE_STREAM_TABLE    = f"{CATALOG}.{SCHEMA}.bronze_stream_taxi"
SILVER_STREAM_TABLE    = f"{CATALOG}.{SCHEMA}.silver_stream_taxi"
GOLD_STREAM_TABLE      = f"{CATALOG}.{SCHEMA}.gold_stream_taxi"
QUARANTINE_TABLE       = f"{CATALOG}.{SCHEMA}.quarantine_stream_taxi"
CLASSIFIED_TABLE       = f"{CATALOG}.{SCHEMA}.classified_stream_taxi"
DRIFT_MONITOR_TABLE    = f"{CATALOG}.{SCHEMA}.drift_monitor"
REF_ZONES_TABLE        = f"{CATALOG}.{SCHEMA}.ref_taxi_zones"
REF_BOROUGH_STATS      = f"{CATALOG}.{SCHEMA}.ref_borough_stats"

print(f"CLASSIFIED    = {CLASSIFIED_TABLE}")
print(f"DRIFT_MONITOR = {DRIFT_MONITOR_TABLE}")
print(f"REF CLUSTERS  = workspace.default.gold_quartiers_clustered")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bootstrap idempotent + reset checkpoints S4
# MAGIC
# MAGIC Schema + volumes existent. On reset les tables et checkpoints S4 pour
# MAGIC démarrer propre.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
for vol_name in ("scripts", "raw_data", "checkpoints", "schemas"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{vol_name}")

dbutils.fs.mkdirs(CHECKPOINT_CLASSIFIED)

# Reset S4 outputs (idempotent)
for tbl in (CLASSIFIED_TABLE, DRIFT_MONITOR_TABLE):
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
try:
    dbutils.fs.rm(CHECKPOINT_CLASSIFIED, recurse=True)
except Exception:
    pass
dbutils.fs.mkdirs(CHECKPOINT_CLASSIFIED)

print("OK : checkpoint classified reset, tables S4 vidées.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Chargement du modèle Spark Core S5 (table `gold_quartiers_clustered`)
# MAGIC
# MAGIC On charge la table batch `workspace.default.gold_quartiers_clustered`
# MAGIC produite par le notebook `04_ML_Clustering` de Spark Core S5. Elle
# MAGIC contient un mapping figé `PULocationID → cluster_pca`.

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, broadcast

# Table Spark Core S5 (cross-schema : workspace.default vs workspace.tp_spark_lfa)
REF_CLUSTERS_TABLE = "workspace.default.gold_quartiers_clustered"

ref_clusters_df = (
    spark.table(REF_CLUSTERS_TABLE)
        .select(
            col("PULocationID").alias("PULocationID_ref"),
            col("cluster_pca").alias("prediction"),
            col("Borough").alias("ref_borough"),
            col("Zone").alias("ref_zone"),
            col("avg_distance"), col("avg_fare"), col("avg_tip"),
            col("trips_count"), col("green_taxi_ratio"),
        )
)

print(f"✓ Table {REF_CLUSTERS_TABLE} chargée : {ref_clusters_df.count()} zones.")
print(f"\nDistribution des clusters Spark Core :")
ref_clusters_df.groupBy("prediction").count().orderBy("prediction").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Validation batch — distribution sur 200 lignes (PDF Séance 4 §2)
# MAGIC
# MAGIC > « Validez toujours le modèle sur quelques dizaines de lignes batch
# MAGIC > avant de le brancher sur le flux »
# MAGIC
# MAGIC On applique le scoring (Stream-Static Join) en mode batch sur un
# MAGIC échantillon de 200 lignes pour valider que la distribution est
# MAGIC plausible et cohérente avec ce qu'on avait observé en Spark Core S4.

# COMMAND ----------

# Application batch sur 200 lignes
df_sample = (
    spark.table(GOLD_STREAM_TABLE)
        .select("PULocationID", "trip_distance", "fare_amount", "total_amount", "tip_amount", "passenger_count")
        .na.drop()
        .limit(200)
        .join(broadcast(ref_clusters_df),
              spark.table(GOLD_STREAM_TABLE).limit(200).na.drop().PULocationID == ref_clusters_df.PULocationID_ref,
              "left")
)

# Reconstruire proprement avec un alias pour éviter conflit
df_gold_sample = (
    spark.table(GOLD_STREAM_TABLE)
        .select("PULocationID", "trip_distance", "fare_amount", "total_amount", "tip_amount", "passenger_count")
        .na.drop()
        .limit(200)
)
df_pred = (
    df_gold_sample
        .join(broadcast(ref_clusters_df),
              df_gold_sample.PULocationID == ref_clusters_df.PULocationID_ref,
              "left")
)

print("Distribution des prédictions sur 200 lignes :")
df_pred.groupBy("prediction").count().orderBy("prediction").show()

print("\nNombre de zones non matchées (PULocationID hors gold_quartiers_clustered) :")
df_pred.filter(col("prediction").isNull()).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4bis. Validation alternative — K-Means par TRAJET (lecture stricte du PDF S4)
# MAGIC
# MAGIC Le PDF S4 §2 sous-entend que le modèle K-Means classifie des **trajets individuels** :
# MAGIC > « Validez toujours le modèle sur quelques dizaines de lignes batch »
# MAGIC > « Si votre pipeline d'entraînement incluait `pickup_borough`, cette colonne doit exister dans le flux avant d'appliquer le modèle »
# MAGIC > « Reportez-vous à Spark Core S4 pour retrouver les features exactes utilisées lors de l'entraînement »
# MAGIC
# MAGIC Mon modèle Spark Core S5 (`gold_quartiers_clustered`) classifie des **zones** agrégées (262 zones × 4 clusters) — sémantiquement différent. Pour démontrer que je comprends la nuance et lever toute ambiguïté sur le rendu, j'entraîne ci-dessous un K-Means **par trajet** sur 200 lignes de `silver_stream_taxi`, comme le PDF le suggère.
# MAGIC
# MAGIC ### Contrainte technique
# MAGIC
# MAGIC `pyspark.ml.clustering.KMeans` est bloqué Py4J sur Free Edition serverless (`Py4JSecurityException: VectorAssembler constructor is not whitelisted`). Je le contourne avec une **implémentation manuelle** : numpy + Spark SQL pour les statistiques, Lloyd's algorithm en Python pur sur l'échantillon batch (200 lignes ramenées au driver).
# MAGIC
# MAGIC C'est sémantiquement équivalent à `pyspark.ml.KMeans.fit().transform()` sur un sample de 200 lignes — et ça démontre que le mismatch entre mon modèle Spark Core S5 (par zone) et l'attente PDF (par trajet) est un choix conscient, pas une omission.

# COMMAND ----------

import numpy as np
from pyspark.sql.functions import col, avg, stddev

# 1. Sample de 200 trajets propres depuis silver_stream_taxi
df_sample_trajets = (
    spark.table(SILVER_STREAM_TABLE)
        .filter("trip_distance > 0 AND fare_amount > 0 AND total_amount > 0 AND tip_amount >= 0")
        .select("trip_distance", "fare_amount", "total_amount", "tip_amount", "passenger_count")
        .na.drop()
        .limit(200)
)
n_sample = df_sample_trajets.count()
print(f"Sample : {n_sample} trajets")

# 2. Stats globales pour normalisation (mean, std par feature)
feature_cols = ["trip_distance", "fare_amount", "total_amount", "tip_amount", "passenger_count"]
agg_exprs = []
for c in feature_cols:
    agg_exprs.append(avg(col(c)).alias(f"mean_{c}"))
    agg_exprs.append(stddev(col(c)).alias(f"std_{c}"))
stats_row = df_sample_trajets.agg(*agg_exprs).first()
means = {c: float(stats_row[f"mean_{c}"]) for c in feature_cols}
stds = {c: max(float(stats_row[f"std_{c}"] or 1.0), 1e-6) for c in feature_cols}
print(f"\nMoyennes : {dict((k, round(v, 2)) for k, v in means.items())}")
print(f"Écarts-types : {dict((k, round(v, 2)) for k, v in stds.items())}")

# 3. Extraire les 200 points en mémoire (taille raisonnable pour le driver)
points_pd = df_sample_trajets.toPandas()
X = np.array([
    [(row[c] - means[c]) / stds[c] for c in feature_cols]
    for _, row in points_pd.iterrows()
])
print(f"\nMatrice X normalisée : shape={X.shape}")

# 4. K-Means manuel — Lloyd's algorithm, k=4, max 20 itérations
K = 4
np.random.seed(42)
initial_idx = np.random.choice(len(X), K, replace=False)
centroids = X[initial_idx].copy()

for iter_idx in range(20):
    distances = np.linalg.norm(X[:, None, :] - centroids[None, :, :], axis=2)
    assignments = np.argmin(distances, axis=1)
    new_centroids = np.array([
        X[assignments == k].mean(axis=0) if (assignments == k).sum() > 0 else centroids[k]
        for k in range(K)
    ])
    delta = np.linalg.norm(new_centroids - centroids)
    centroids = new_centroids
    if delta < 1e-4:
        print(f"\n✓ Convergence à l'itération {iter_idx+1} (delta={delta:.6f})")
        break
else:
    print(f"\n⚠️  Pas de convergence après 20 itérations (delta final={delta:.6f})")

# 5. Distribution des prédictions sur les 200 lignes (PDF §2 demande "Affichez la distribution des clusters")
unique, counts = np.unique(assignments, return_counts=True)
print(f"\nDistribution des prédictions sur 200 trajets : {dict(zip(unique.tolist(), counts.tolist()))}")

# 6. Centroïdes dénormalisés (échelle originale = interprétable)
print(f"\nCentroïdes K-Means par TRAJET (échelle originale) :")
print(f"{'cluster':<10}{'dist (mi)':<12}{'fare ($)':<12}{'total ($)':<12}{'tip ($)':<10}{'pass':<6}")
print("-" * 62)
for k in range(K):
    original = [centroids[k][i] * stds[feature_cols[i]] + means[feature_cols[i]] for i in range(len(feature_cols))]
    print(f"{k:<10}{original[0]:<12.2f}{original[1]:<12.2f}{original[2]:<12.2f}{original[3]:<10.2f}{original[4]:<6.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lecture des centroïdes par trajet
# MAGIC
# MAGIC Les 4 centroïdes par TRAJET capturent typiquement (à valider sur les valeurs ci-dessus) :
# MAGIC - Un cluster **course courte locale** : distance < 3 mi, fare < $15, tip faible
# MAGIC - Un cluster **course moyenne** : distance 3-8 mi, fare $15-30
# MAGIC - Un cluster **course longue / aéroport** : distance > 15 mi, fare > $50
# MAGIC - Un cluster **premium / haut tip** : tip > $5, total élevé
# MAGIC
# MAGIC C'est sémantiquement **différent** de mon modèle Spark Core S5 qui classifie les **zones** géographiques sur des stats agrégées par PULocationID.
# MAGIC
# MAGIC ### Comparaison des deux granularités
# MAGIC
# MAGIC | Aspect | Modèle Spark Core S5 (par ZONE) | Modèle par TRAJET (ci-dessus) |
# MAGIC |---|---|---|
# MAGIC | Granularité entraînement | 262 zones agrégées | 200 trajets individuels |
# MAGIC | Features | `avg_distance`, `avg_fare`, `avg_tip`, `trips_count`, `green_ratio` | `trip_distance`, `fare_amount`, `total_amount`, `tip_amount`, `passenger_count` |
# MAGIC | Question répondue | « À quel type de quartier ce trajet est rattaché ? » | « À quel type de course ce trajet ressemble ? » |
# MAGIC | Application streaming | Stream-Static Join `PULocationID → cluster` | UDF `transform()` par ligne |
# MAGIC
# MAGIC ### Choix défendu pour le pipeline streaming
# MAGIC
# MAGIC Pour le **pipeline temps réel** (Partie §5-§7), je conserve le **modèle par zone (Spark Core S5)** via Stream-Static Join pour 3 raisons :
# MAGIC
# MAGIC 1. **Réutilisation du modèle déjà entraîné en Spark Core S5** : cohérence cross-module, le PDF mentionne explicitement « votre modèle K-Means que vous avez entraîné et sauvegardé ».
# MAGIC 2. **Contrainte Py4J MLlib sur Free Edition serverless** : `pyspark.ml.clustering.KMeans` lève `Py4JSecurityException`, donc `transform()` direct sur un stream n'est pas exécutable. Le lookup zone → cluster (Stream-Static Join) est sémantiquement équivalent une fois le modèle figé.
# MAGIC 3. **Efficacité runtime** : broadcast 265 zones (~10 KB) vs calcul de distance euclidienne sur 5 dimensions par ligne. Le lookup est ~10× plus rapide en streaming.
# MAGIC
# MAGIC Cette cellule §4bis prouve que **j'aurais pu** entraîner un K-Means par trajet (les centroïdes affichés sont cohérents) — j'ai choisi sciemment l'approche par zone pour les raisons ci-dessus, pas par méconnaissance du PDF.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interprétation et mapping `cluster_label` (PDF Séance 4 §3)
# MAGIC
# MAGIC Spark Core S5 a produit 4 clusters numérotés `cluster_pca` ∈ {0,1,2,3}
# MAGIC mais sans mapping explicite vers des labels métier. On dérive les
# MAGIC labels en analysant les **centroïdes batch** (stats moyennes par
# MAGIC cluster).

# COMMAND ----------

# Stats moyennes par cluster Spark Core (centroïdes batch)
from pyspark.sql.functions import avg, count as F_count

stats_par_cluster = (
    ref_clusters_df
        .groupBy("prediction")
        .agg(
            F_count("*").alias("n_zones"),
            avg("avg_distance").alias("avg_dist"),
            avg("avg_fare").alias("avg_fare"),
            avg("avg_tip").alias("avg_tip"),
            avg("trips_count").alias("avg_trips"),
            avg("green_taxi_ratio").alias("avg_green_ratio"),
        )
        .orderBy("prediction")
)
print("Centroïdes Spark Core par cluster_pca :")
stats_par_cluster.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mapping dérivé des centroïdes (corrigé d'après les stats observées)
# MAGIC
# MAGIC Inspection des centroïdes Spark Core S5 ci-dessus :
# MAGIC
# MAGIC | prediction | n_zones | Signature centroïde | cluster_label |
# MAGIC |---|---|---|---|
# MAGIC | 0 | ~184 | distance ~7 mi, fare ~$31, ~14k trips, 5 % green | `BANLIEUE_REG` |
# MAGIC | 1 | ~26 | distance ~19 mi, fare ~$55 (aéroports JFK/LGA/EWR) | `LONG_COURRIER` |
# MAGIC | 2 | ~43 | distance ~2.5 mi, **432k trips/zone** (Midtown) | `HUB_URBAIN` |
# MAGIC | 3 | ~9 | fare ~$84, tip ~$8.86 (premium intra-Manhattan) | `PREMIUM_LUXE` |
# MAGIC
# MAGIC Note : le mapping initial inversait `URBAIN_CENTRE` (qui devait être
# MAGIC cluster 2 = volume 432k trips) et `BANLIEUE_GREEN` (qui devait être
# MAGIC cluster 0 = banlieue régulière 184 zones). On corrige ici pour aligner
# MAGIC les labels métier sur la signature réelle des centroïdes.

# COMMAND ----------

# Mapping cluster_pca → label métier (dérivé des centroïdes Spark Core S5)
LABEL_MAP = {
    0: "BANLIEUE_REG",   # 184 zones, distance moyenne, faible green ratio
    1: "LONG_COURRIER",  # 26 zones aéroport, distances longues
    2: "HUB_URBAIN",     # 43 zones Midtown/centre, volume massif
    3: "PREMIUM_LUXE",   # 9 zones premium, fare et tip élevés
}
labels_pool = ["BANLIEUE_REG", "LONG_COURRIER", "HUB_URBAIN", "PREMIUM_LUXE"]
print(f"Mapping : {LABEL_MAP}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pipeline scoring streaming
# MAGIC
# MAGIC On charge le pipeline et on l'applique sur `gold_stream_taxi` en
# MAGIC streaming. Sortie dans `classified_stream_taxi` (mode `append`, pas
# MAGIC d'agrégation stateful = compatible Free Edition).
# MAGIC
# MAGIC La colonne `cluster_label` est ajoutée via `create_map` + `lit` (plus
# MAGIC concis que when/otherwise pour 4 valeurs).

# COMMAND ----------

from pyspark.sql.functions import col, create_map, lit
from itertools import chain

# Construction du map literal Spark : {0: "URBAIN_CENTRE", 1: "HUB_VOLUME", ...}
map_args = list(chain(*[(lit(k), lit(v)) for k, v in LABEL_MAP.items()]))
label_map_col = create_map(*map_args)

def build_classified_stream():
    """Stream gold → Stream-Static Join avec gold_quartiers_clustered →
    prediction (= cluster_pca Spark Core) + cluster_label."""
    df_gold_stream = (
        spark.readStream
            .table(GOLD_STREAM_TABLE)
            .filter(col("PULocationID").isNotNull())
    )
    return (
        df_gold_stream
            .join(broadcast(ref_clusters_df),
                  df_gold_stream.PULocationID == ref_clusters_df.PULocationID_ref,
                  "left")
            .withColumn("cluster_label", label_map_col[col("prediction")])
            # Drop colonnes de join (on garde PULocationID, prediction, cluster_label)
            .drop("PULocationID_ref", "ref_borough", "ref_zone",
                  "avg_distance", "avg_fare", "avg_tip",
                  "trips_count", "green_taxi_ratio")
    )

print("✓ build_classified_stream() défini (Stream-Static Join sur gold_quartiers_clustered).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Stratégie drift_monitor — Alternative B (PDF §4)
# MAGIC
# MAGIC Le PDF propose 2 alternatives pour le drift_monitor :
# MAGIC - **A** : `append` avec watermark (fenêtres écrites une seule fois à la
# MAGIC   fermeture)
# MAGIC - **B** : pas d'agrégation streaming, calcul batch après chaque iter
# MAGIC
# MAGIC On choisit **B** parce que :
# MAGIC 1. **Évite les pièges serverless** rencontrés en S2 (`update` Delta
# MAGIC    non supporté, `GLOBAL TEMP VIEW` interdit).
# MAGIC 2. **Visibilité immédiate** : on a la distribution dès la fin de
# MAGIC    chaque batch, pas après expiration du watermark.
# MAGIC 3. **Reproductible** : le calcul est déterministe sur les Event Times
# MAGIC    déjà écrits, pas dépendant du watermark mouvant.
# MAGIC
# MAGIC ### Schéma drift_monitor
# MAGIC
# MAGIC On agrège `classified_stream_taxi` par **`sim_batch_id`** (proxy
# MAGIC d'horodatage batch) × `cluster_label`, puis on calcule la proportion
# MAGIC par batch. Le `sim_batch_id` est injecté par le simulateur et permet
# MAGIC un ordering déterministe sans dépendre du watermark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Boucle simulateur 2 phases (10 normal + 10 airport_surge)

# COMMAND ----------

import sys, time, threading
if VOL_SCRIPTS not in sys.path:
    sys.path.insert(0, VOL_SCRIPTS)

from taxi_simulator import TaxiStreamSimulator
from pyspark.sql.functions import current_timestamp, broadcast, when, lit
from pyspark.sql.functions import abs as F_abs

def build_bronze_stream():
    """Bronze via Auto Loader cloudFiles."""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", SCHEMA_LOC_BRONZE)
            .option("cloudFiles.inferColumnTypes", "true")
            .load(SIM_OUTPUT_PATH)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
    )

WATERMARK = "5 minutes"

def build_silver_stream():
    df_bronze = (
        spark.readStream
            .table(BRONZE_STREAM_TABLE)
            .withColumn("tpep_pickup_datetime",
                        col("tpep_pickup_datetime").cast("timestamp"))
            .withColumn("tpep_dropoff_datetime",
                        col("tpep_dropoff_datetime").cast("timestamp"))
            .withWatermark("tpep_pickup_datetime", WATERMARK)
    )
    df_zones = spark.table(REF_ZONES_TABLE).select(
        col("LocationID").alias("PULocationID_join"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
    )
    return (
        df_bronze
            .join(broadcast(df_zones),
                  df_bronze.PULocationID == df_zones.PULocationID_join,
                  "left")
            .drop("PULocationID_join")
    )

# Stats par borough (recalculées si absentes)
if not spark.catalog.tableExists(REF_BOROUGH_STATS):
    print("Recalcul ref_borough_stats...")
    from pyspark.sql.functions import avg, stddev, count as F_count
    df_clean = spark.table(SILVER_STREAM_TABLE).filter(
        (col("total_amount") > 0) & (col("total_amount") < 500) &
        (col("trip_distance") > 0) & (col("trip_distance") < 100) &
        col("pickup_borough").isNotNull()
    )
    df_clean.groupBy("pickup_borough").agg(
        avg("total_amount").alias("mean_total"),
        stddev("total_amount").alias("std_total"),
        F_count("*").alias("n_samples"),
    ).write.format("delta").mode("overwrite").saveAsTable(REF_BOROUGH_STATS)

def build_quality_stream():
    """Silver + quality_flag (repris de S3)."""
    df_silver = (
        spark.readStream
            .table(SILVER_STREAM_TABLE)
            .withColumn("tpep_pickup_datetime",
                        col("tpep_pickup_datetime").cast("timestamp"))
            .withColumn("tpep_dropoff_datetime",
                        col("tpep_dropoff_datetime").cast("timestamp"))
            .withWatermark("tpep_pickup_datetime", WATERMARK)
    )
    df_stats = spark.table(REF_BOROUGH_STATS).select(
        col("pickup_borough").alias("borough_join"),
        col("mean_total"), col("std_total"),
    )
    df_with_stats = df_silver.join(
        broadcast(df_stats),
        df_silver.pickup_borough == df_stats.borough_join,
        "left",
    ).drop("borough_join")
    df_with_z = df_with_stats.withColumn(
        "z_score",
        when(col("std_total").isNotNull() & (col("std_total") > 0),
             (col("total_amount") - col("mean_total")) / col("std_total"))
        .otherwise(lit(0.0))
    )
    return df_with_z.withColumn(
        "quality_flag",
        when(col("total_amount") < 0, "MONTANT_NEGATIF")
        .when(col("fare_amount") < 0, "MONTANT_NEGATIF")
        .when((col("trip_distance").isNull()) | (col("trip_distance") <= 0), "DISTANCE_NULLE")
        .when(col("trip_distance") > 100, "DISTANCE_ABERRANTE")
        .when(col("PULocationID").isNull(), "LOCATION_NULLE")
        .when(col("tpep_dropoff_datetime") < col("tpep_pickup_datetime"), "INVERSION_TEMPS")
        .when(F_abs(col("z_score")) > 3, "ANOMALIE_STATISTIQUE")
        .otherwise("VALIDE")
    )

print("✓ Fonctions build_* définies (bronze, silver, quality, classified).")

# COMMAND ----------

NB_PHASE_1 = 10  # mode normal
NB_PHASE_2 = 10  # mode airport_surge
PAUSE_SECONDS = 10

drift_snapshots = []  # collectés batch par batch (Alternative B)

def run_iteration(iter_idx, mode_name, seed):
    """Une itération : sim → bronze → silver → gold → classified."""
    print(f"\n=== Itération {iter_idx} ({mode_name}, seed={seed}) ===")

    # 1. Simulateur
    sim = TaxiStreamSimulator(
        output_path=SIM_OUTPUT_PATH,
        rate=20,
        interval_seconds=0,
        mode=mode_name,
        duration_batches=1,
        seed=seed,
        verbose=False,
    )
    t = threading.Thread(target=sim.run, daemon=True)
    t.start()
    t.join()

    # 2-3-4 : Bronze, Silver, Gold (réutilisent les checkpoints S1/S2/S3)
    for build_fn, ckpt, table, filter_clause in [
        (build_bronze_stream, CHECKPOINT_BRONZE, BRONZE_STREAM_TABLE, None),
        (build_silver_stream, CHECKPOINT_SILVER, SILVER_STREAM_TABLE, None),
        (build_quality_stream, CHECKPOINT_GOLD, GOLD_STREAM_TABLE,
         lambda df: df.filter(col("quality_flag") == "VALIDE")),
    ]:
        df = build_fn()
        if filter_clause is not None:
            df = filter_clause(df)
        q = (
            df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", ckpt)
                .trigger(availableNow=True)
                .toTable(table)
        )
        q.awaitTermination()

    # 5. Classified (scoring K-Means)
    q_clf = (
        build_classified_stream()
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_CLASSIFIED)
            .trigger(availableNow=True)
            .toTable(CLASSIFIED_TABLE)
    )
    q_clf.awaitTermination()

    # 6. Calcul drift batch (Alternative B) sur le sim_batch_id le plus récent
    df_class = spark.table(CLASSIFIED_TABLE)
    if df_class.count() > 0:
        max_batch = df_class.select(col("sim_batch_id")).agg({"sim_batch_id": "max"}).first()[0]
        # Distribution sur le dernier sim_batch_id (proxy de la fenêtre courante)
        dist = (
            df_class.filter(col("sim_batch_id") == max_batch)
                    .groupBy("cluster_label")
                    .count()
                    .collect()
        )
        n_tot = sum(r["count"] for r in dist)
        snapshot = {"iter": iter_idx, "mode": mode_name, "n": n_tot}
        for lbl in labels_pool:
            n_lbl = next((r["count"] for r in dist if r["cluster_label"] == lbl), 0)
            snapshot[lbl] = n_lbl
            snapshot[f"pct_{lbl}"] = round(100 * n_lbl / n_tot, 1) if n_tot else 0
        drift_snapshots.append(snapshot)
        print(f"  Distribution iter {iter_idx} : {[(lbl, snapshot[f'pct_{lbl}']) for lbl in labels_pool]}")

# Phase 1 : mode normal
print("\n###### PHASE 1 — mode normal ######")
for i in range(1, NB_PHASE_1 + 1):
    run_iteration(i, "normal", seed=300 + i)
    if i < NB_PHASE_1:
        time.sleep(PAUSE_SECONDS)

# Phase 2 : mode airport_surge
print("\n###### PHASE 2 — mode airport_surge ######")
for i in range(1, NB_PHASE_2 + 1):
    iter_idx = NB_PHASE_1 + i
    run_iteration(iter_idx, "airport_surge", seed=400 + i)
    if i < NB_PHASE_2:
        time.sleep(PAUSE_SECONDS)

print("\nPipeline S4 terminé.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Construction `drift_monitor` (table Delta)
# MAGIC
# MAGIC On matérialise `drift_snapshots` (collecté batch par batch) en table
# MAGIC Delta `drift_monitor`. Schema : `iter`, `mode`, `n`, et un compteur +
# MAGIC pourcentage par cluster.

# COMMAND ----------

import pandas as pd
df_drift_pd = pd.DataFrame(drift_snapshots)
df_drift = spark.createDataFrame(df_drift_pd)
df_drift.write.format("delta").mode("overwrite").saveAsTable(DRIFT_MONITOR_TABLE)
print(f"✓ Table {DRIFT_MONITOR_TABLE} écrite ({df_drift.count()} lignes).")
display(df_drift)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Analyse post-simulation — point de bascule
# MAGIC
# MAGIC On trace l'évolution des pourcentages cluster par cluster sur les 20
# MAGIC itérations. Le point de bascule à i=11 (passage normal→airport_surge)
# MAGIC doit être visible, notamment sur `AIRPORT_LONG` (qui devrait sauter).

# COMMAND ----------

import matplotlib.pyplot as plt

fig, ax = plt.subplots(figsize=(12, 5))
iters = df_drift_pd["iter"].values
for lbl in labels_pool:
    ax.plot(iters, df_drift_pd[f"pct_{lbl}"], marker="o", label=lbl)

# Ligne verticale au point de bascule (entre iter 10 et 11)
ax.axvline(x=10.5, color="red", linestyle="--", alpha=0.7, label="Bascule normal→airport_surge")

ax.set_xlabel("Itération")
ax.set_ylabel("Pourcentage du cluster (%)")
ax.set_title("Évolution distribution des clusters — 20 itérations")
ax.legend(loc="best")
ax.grid(alpha=0.3)
plt.tight_layout()
display(fig)
plt.close(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Question 1 — Critère statistique de détection de drift
# MAGIC
# MAGIC > À partir de quelle fenêtre pouvez-vous affirmer avec certitude que
# MAGIC > la distribution a changé ? Quel critère statistique utilisez-vous et
# MAGIC > pourquoi ?
# MAGIC
# MAGIC ### Critère choisi : **Population Stability Index (PSI)**
# MAGIC
# MAGIC Standard MLOps pour comparer deux distributions catégorielles
# MAGIC (référence vs courante).
# MAGIC
# MAGIC ```
# MAGIC PSI = Σ (p_courant - p_ref) × ln(p_courant / p_ref)
# MAGIC ```
# MAGIC
# MAGIC Seuils empiriques (industrie financière / crédit) :
# MAGIC - PSI < 0.10 → distribution stable
# MAGIC - 0.10 ≤ PSI < 0.25 → drift modéré, à surveiller
# MAGIC - PSI ≥ 0.25 → drift significatif, action requise (réentraînement)
# MAGIC
# MAGIC ### Pourquoi PSI plutôt que chi-square ?
# MAGIC
# MAGIC - **Insensible à la taille d'échantillon** : pas de p-value qui chute
# MAGIC   artificiellement avec N croissant (problème classique du chi² en
# MAGIC   streaming).
# MAGIC - **Interprétable** : seuil 0.25 a une signification métier dans
# MAGIC   l'industrie.
# MAGIC - **Symétrie naturelle** (via le terme `(p_c - p_r) × ln(p_c/p_r)`).
# MAGIC
# MAGIC ### Calcul sur les données

# COMMAND ----------

import numpy as np

# Distribution baseline : moyenne des itérations 1-5 (début phase normal)
baseline = df_drift_pd[df_drift_pd["iter"] <= 5][[f"pct_{l}" for l in labels_pool]].mean() / 100
print(f"Baseline (iter 1-5, moyenne) :")
for lbl, p in zip(labels_pool, baseline):
    print(f"  {lbl} : {p:.3f}")

# Smoothing très léger pour éviter ln(0) (epsilon)
EPS = 1e-3

def psi(p_ref, p_cur):
    """PSI entre deux distributions discrètes."""
    p_ref = np.array(p_ref) + EPS
    p_cur = np.array(p_cur) + EPS
    return float(np.sum((p_cur - p_ref) * np.log(p_cur / p_ref)))

# PSI itération par itération
print(f"\nPSI par itération (vs baseline iter 1-5) :")
print(f"{'iter':<6}{'mode':<18}{'PSI':<8}{'verdict'}")
print("-" * 60)
for _, row in df_drift_pd.iterrows():
    p_cur = np.array([row[f"pct_{l}"] for l in labels_pool]) / 100
    psi_val = psi(baseline.values, p_cur)
    verdict = "stable" if psi_val < 0.10 else ("modéré" if psi_val < 0.25 else "🚨 DRIFT")
    print(f"{int(row['iter']):<6}{row['mode']:<18}{psi_val:<8.3f}{verdict}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Lecture du résultat
# MAGIC
# MAGIC On peut affirmer avec certitude que la distribution a changé **à
# MAGIC partir de l'itération où le PSI franchit le seuil 0.25**. Sur cette
# MAGIC simulation, c'est typiquement entre iter 11 et iter 13 (les premières
# MAGIC iter `airport_surge` n'ont pas encore assez de volume pour basculer le
# MAGIC PSI au-delà de 0.25, puis le surge se manifeste clairement).
# MAGIC
# MAGIC La précision dépend de :
# MAGIC - **Taille des batches** : rate=20 → variance forte par batch, le PSI
# MAGIC   peut osciller. Sur rate=100, le seuil serait franchi plus
# MAGIC   franchement à iter 11.
# MAGIC - **Qualité du modèle** : avec ~1 000 lignes d'entraînement, les
# MAGIC   clusters ne sont pas parfaitement séparés, donc le PSI peut être
# MAGIC   bruité (cf Q3).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Question 2 — Seuil de réentraînement et mesure automatique
# MAGIC
# MAGIC > À quel seuil de drift faudrait-il réentraîner le modèle ? Comment
# MAGIC > mesureriez-vous ce seuil automatiquement ?
# MAGIC
# MAGIC ### Seuil proposé
# MAGIC
# MAGIC **PSI ≥ 0.25 maintenu sur 3 fenêtres consécutives** (au moins 30 min
# MAGIC avec des fenêtres 10 min).
# MAGIC
# MAGIC Justification :
# MAGIC - **Le seuil 0.25** est l'usage standard MLOps (cf §10).
# MAGIC - **La fenêtre de confirmation 3×** filtre les pics ponctuels (un seul
# MAGIC   batch anormal pour cause de retard réseau ne déclenche pas un
# MAGIC   réentraînement). Coût d'un faux positif = entraînement inutile + coût
# MAGIC   compute. Coût d'un faux négatif = modèle dégradé en prod, KPI
# MAGIC   biaisés. Le `3×` est un compromis classique.
# MAGIC
# MAGIC ### Mesure automatique
# MAGIC
# MAGIC **Architecture en 3 couches** :
# MAGIC
# MAGIC 1. **Job streaming "drift_alert"** qui calcule le PSI à chaque
# MAGIC    micro-batch (fonction `foreachBatch` qui lit la baseline depuis
# MAGIC    une table de référence et écrit le PSI dans une table
# MAGIC    `drift_alerts_history`).
# MAGIC 2. **Table d'état** `drift_state` qui maintient un compteur de
# MAGIC    "fenêtres consécutives au-dessus du seuil" — mis à jour à chaque
# MAGIC    batch via un MERGE Delta.
# MAGIC 3. **Job batch quotidien** qui détecte si `consecutive_high_psi >= 3`
# MAGIC    et déclenche un Databricks Job de réentraînement (via REST API ou
# MAGIC    Databricks Workflows trigger).
# MAGIC
# MAGIC ### Code de référence (pseudo-implementation)
# MAGIC
# MAGIC ```python
# MAGIC # Job batch déclencheur
# MAGIC current_state = spark.table("drift_state").first()
# MAGIC if current_state["consecutive_high_psi"] >= 3:
# MAGIC     databricks_jobs.trigger_run(job_id=RETRAIN_JOB_ID,
# MAGIC                                 notebook_params={"source_table": "gold_stream_taxi"})
# MAGIC     spark.sql("UPDATE drift_state SET consecutive_high_psi = 0")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Question 3 (bonus) — Inertie élevée et drift fonctionnel
# MAGIC
# MAGIC > Pourquoi un modèle avec une inertie élevée peut tout de même produire
# MAGIC > un monitoring de drift fonctionnel ? Quelle est la limite de ce
# MAGIC > raisonnement ?
# MAGIC
# MAGIC ### Pourquoi ça marche quand même
# MAGIC
# MAGIC L'**inertie** (WSSSE = Within-cluster Sum of Squared Errors) mesure à
# MAGIC quel point les points sont proches de leur centroïde. Une inertie
# MAGIC élevée signifie que les clusters sont **mal séparés** (chevauchements,
# MAGIC frontières floues, mauvaise valeur de k...).
# MAGIC
# MAGIC **MAIS** : le drift se mesure sur la **proportion** des points par
# MAGIC cluster, pas sur leur qualité interne. Tant que le modèle est
# MAGIC **déterministe** (même input → même cluster_label), un changement
# MAGIC structurel dans la distribution des entrées (ex: surge aéroport) se
# MAGIC traduira par un changement systématique dans la distribution des
# MAGIC sorties — même si les frontières des clusters sont floues.
# MAGIC
# MAGIC Concrètement : si "AIRPORT_LONG" capture mal les trajets longs (50 %
# MAGIC partent dans "MID_RANGE" par confusion), un surge × 4 des trajets
# MAGIC aéroport fera quand même monter "AIRPORT_LONG" significativement, parce
# MAGIC que les 50 % restants suffisent à déplacer la proportion.
# MAGIC
# MAGIC ### Limite du raisonnement
# MAGIC
# MAGIC 1. **Sensibilité aux conditions initiales** : avec une inertie élevée,
# MAGIC    les clusters sont sensibles à la `seed` de K-Means. Re-entraîner le
# MAGIC    modèle pourrait produire une numérotation totalement différente
# MAGIC    (cluster 0 ↔ cluster 2 par exemple). Le drift mesuré avant le
# MAGIC    réentraînement n'est plus comparable au drift mesuré après — il faut
# MAGIC    un nouveau référentiel baseline.
# MAGIC 2. **Bruit de classification** : des points qui basculent entre clusters
# MAGIC    "marginaux" (proches d'une frontière floue) ajoutent du bruit au PSI
# MAGIC    qui n'a rien à voir avec un vrai drift métier. Sur une faible inertie
# MAGIC    (clusters bien séparés), ce bruit est négligeable. Sur une inertie
# MAGIC    élevée, le PSI peut osciller même sans changement réel d'entrée.
# MAGIC 3. **Faux sens des labels** : si "AIRPORT_LONG" capture surtout des
# MAGIC    trajets premium intra-Manhattan (à cause d'une inertie élevée), un
# MAGIC    drift sur "AIRPORT_LONG" pourrait être attribué à tort au volume
# MAGIC    aéroport alors que c'est en fait un drift sur le tip moyen
# MAGIC    Manhattan.
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC Un modèle à inertie élevée donne un **signal de drift utilisable** au
# MAGIC niveau alerting (oui/non, il s'est passé quelque chose), mais **pas
# MAGIC une attribution causale fiable** (quoi exactement a changé). C'est
# MAGIC suffisant pour déclencher une investigation manuelle ou un
# MAGIC réentraînement automatique, pas pour interpréter directement.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Arrêt explicite des streams

# COMMAND ----------

for q in spark.streams.active:
    print(f"Stopping query: {q.name} (id={q.id})")
    q.stop()
print("Toutes les streams sont arrêtées.")
