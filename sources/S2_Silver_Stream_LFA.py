# Databricks notebook source
# MAGIC %md
# MAGIC # S2 — Silver Stream + transformations temporelles
# MAGIC
# MAGIC **Auteur** : Lorenzo Faloci — trigramme `LFA`
# MAGIC **Cours** : Spark Structured Streaming — Séance 2
# MAGIC **Date** : 2026-05-19
# MAGIC
# MAGIC ## Objectif
# MAGIC
# MAGIC Construire deux jobs streaming distincts à partir de `bronze_stream_taxi` (S1) :
# MAGIC
# MAGIC 1. **Job Silver** : typage + watermark sur Event Time + Stream-Static Join
# MAGIC    avec `ref_taxi_zones` pour enrichir avec `pickup_borough`. Écrit dans
# MAGIC    `silver_stream_taxi`.
# MAGIC 2. **Job KPI** : agrégation par borough × fenêtre 10 min (count trajets +
# MAGIC    avg montant). Écrit dans `kpi_stream_by_window` en mode `update`.
# MAGIC
# MAGIC ## Paramètre imposé
# MAGIC
# MAGIC - Simulateur : `late_data_ratio = 0.15` (15% des trajets avec retard 1-10 min).
# MAGIC
# MAGIC ## Décisions techniques (justifiées dans les sections dédiées)
# MAGIC
# MAGIC | Décision | Valeur | Section |
# MAGIC |---|---|---|
# MAGIC | Watermark | **5 minutes** | §4 |
# MAGIC | Mode sortie Silver | `append` | §4 |
# MAGIC | Mode sortie KPI | **`append`** *(cible `update`, voir justification)* | §5 |
# MAGIC | Trigger | `availableNow=True` | §4 (contrainte serverless) |
# MAGIC | Checkpoints | 3 dossiers distincts (bronze, silver, kpi) | §4-§5 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Paramètres

# COMMAND ----------

TRIGRAMME = "LFA"

CATALOG = "workspace"
SCHEMA  = f"tp_spark_{TRIGRAMME.lower()}"

# Volumes UC (créés en S1, idempotent)
VOL_SCRIPTS     = f"/Volumes/{CATALOG}/{SCHEMA}/scripts"
VOL_RAW_DATA    = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
VOL_CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
VOL_SCHEMAS     = f"/Volumes/{CATALOG}/{SCHEMA}/schemas"

# Chemins applicatifs
SIM_OUTPUT_PATH       = f"{VOL_RAW_DATA}/stream_input/"
ZONES_CSV_PATH        = f"{VOL_RAW_DATA}/taxi_zones.csv"

CHECKPOINT_BRONZE     = f"{VOL_CHECKPOINTS}/bronze_stream/"
CHECKPOINT_SILVER     = f"{VOL_CHECKPOINTS}/silver_stream/"
CHECKPOINT_KPI        = f"{VOL_CHECKPOINTS}/kpi_stream/"

SCHEMA_LOC_BRONZE     = f"{VOL_SCHEMAS}/bronze_stream/"

# Tables
BRONZE_STREAM_TABLE   = f"{CATALOG}.{SCHEMA}.bronze_stream_taxi"
SILVER_STREAM_TABLE   = f"{CATALOG}.{SCHEMA}.silver_stream_taxi"
KPI_STREAM_TABLE      = f"{CATALOG}.{SCHEMA}.kpi_stream_by_window"
REF_ZONES_TABLE       = f"{CATALOG}.{SCHEMA}.ref_taxi_zones"

print(f"BRONZE = {BRONZE_STREAM_TABLE}")
print(f"SILVER = {SILVER_STREAM_TABLE}")
print(f"KPI    = {KPI_STREAM_TABLE}")
print(f"ZONES  = {REF_ZONES_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bootstrap idempotent
# MAGIC
# MAGIC Schema + volumes ont été créés en S1. Cette cellule est un no-op si on
# MAGIC relance.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
for vol_name in ("scripts", "raw_data", "checkpoints", "schemas"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{vol_name}")

dbutils.fs.mkdirs(SIM_OUTPUT_PATH)
dbutils.fs.mkdirs(CHECKPOINT_BRONZE)
dbutils.fs.mkdirs(CHECKPOINT_SILVER)
dbutils.fs.mkdirs(CHECKPOINT_KPI)
dbutils.fs.mkdirs(SCHEMA_LOC_BRONZE)

# Reset KPI uniquement (changement de outputMode update → append entre tentatives).
# Bronze + Silver checkpoints sont conservés (idempotence : on continue d'ajouter).
spark.sql(f"DROP TABLE IF EXISTS {KPI_STREAM_TABLE}")
try:
    dbutils.fs.rm(CHECKPOINT_KPI, recurse=True)
except Exception:
    pass
dbutils.fs.mkdirs(CHECKPOINT_KPI)

print("OK : schema + volumes + checkpoints prêts (KPI reset).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Étape préalable — charger `ref_taxi_zones` (table statique)
# MAGIC
# MAGIC Source : `taxi_zones.csv` fourni par le prof (265 lignes : `LocationID`,
# MAGIC `Borough`, `Zone`, `service_zone`). On le lit depuis le volume `raw_data/`
# MAGIC (à uploader manuellement avant exécution, cf §3.bis) et on le matérialise
# MAGIC en table Delta `ref_taxi_zones`.
# MAGIC
# MAGIC Cette table sert au **Stream-Static Join** du Job Silver (§4) pour ajouter
# MAGIC `pickup_borough` à chaque trajet via `PULocationID = LocationID`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.bis — Action manuelle : déposer le CSV des zones
# MAGIC
# MAGIC Avant d'exécuter la cellule ci-dessous, **uploader `taxi_zones.csv`** dans
# MAGIC `/Volumes/workspace/tp_spark_lfa/raw_data/` via Catalog UI (Upload to this
# MAGIC volume).

# COMMAND ----------

import os
assert os.path.exists(ZONES_CSV_PATH), (
    f"❌ CSV absent : {ZONES_CSV_PATH}\n"
    f"   → uploader taxi_zones.csv dans /Volumes/.../raw_data/ via Catalog UI."
)
print(f"✓ CSV présent : {ZONES_CSV_PATH}")

# COMMAND ----------

from pyspark.sql.functions import col

# Création de ref_taxi_zones si absente
if not spark.catalog.tableExists(REF_ZONES_TABLE):
    df_zones = (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(ZONES_CSV_PATH)
    )
    df_zones.write.format("delta").mode("overwrite").saveAsTable(REF_ZONES_TABLE)
    print(f"✓ Table {REF_ZONES_TABLE} créée ({df_zones.count()} lignes).")
else:
    print(f"ℹ️  Table {REF_ZONES_TABLE} déjà présente.")

# Sanity check
spark.table(REF_ZONES_TABLE).show(5, truncate=False)
print(f"Total zones : {spark.table(REF_ZONES_TABLE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Job 1 — Silver streaming
# MAGIC
# MAGIC ### Watermark : 5 minutes — justification
# MAGIC
# MAGIC Le simulateur injecte un retard **uniforme entre 1 et 10 minutes** sur
# MAGIC 15% des trajets (`late_data_ratio=0.15`, défauts
# MAGIC `late_data_min_minutes=1`, `late_data_max_minutes=10`).
# MAGIC
# MAGIC Choix `watermark = "5 minutes"` :
# MAGIC
# MAGIC - **Capture théorique** des retards 1-5 min : `(5-1)/(10-1) ≈ 44%` des
# MAGIC   trajets tardifs gardés.
# MAGIC - **Perte théorique attendue** : `15% × (10-5)/(10-1) ≈ 15% × 55.5% ≈ 8.3%`
# MAGIC   des trajets totaux.
# MAGIC - **Compromis mémoire** : 5 min est le centre de la fourchette
# MAGIC   recommandée par le cours (2-5 min). Suffisant pour ne pas saturer la
# MAGIC   RAM du serverless single-node.
# MAGIC
# MAGIC ### Trigger : `availableNow=True`
# MAGIC
# MAGIC Idem S1 : compute serverless du Free Edition interdit `processingTime`.
# MAGIC
# MAGIC ### Checkpoint : `/Volumes/.../checkpoints/silver_stream/`
# MAGIC
# MAGIC Distinct de celui du Bronze (S1) et du KPI (§5). Règle : 1 query = 1
# MAGIC checkpoint isolé.
# MAGIC
# MAGIC ### Mode sortie : `append`
# MAGIC
# MAGIC Pas d'agrégation stateful → `append` suffit. Les lignes valides
# MAGIC (event_time > watermark) sont écrites une seule fois et ne sont jamais
# MAGIC modifiées.

# COMMAND ----------

from pyspark.sql.functions import broadcast, col

WATERMARK = "5 minutes"

def build_silver_stream():
    """DataFrame streaming Silver : typage + watermark + Stream-Static Join.

    Note : on cast `tpep_pickup_datetime` en TIMESTAMP (avec TZ) avant
    `withWatermark`. Default Databricks DBR récent = TIMESTAMP_NTZ (no
    timezone) mais Spark Streaming exige TIMESTAMP pour le watermark
    (`EVENT_TIME_IS_NOT_ON_TIMESTAMP_TYPE` sinon).
    """
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Job 2 — KPI par fenêtre × borough
# MAGIC
# MAGIC ### Agrégation : fenêtre tumbling 10 min × `pickup_borough`
# MAGIC
# MAGIC `groupBy(window("tpep_pickup_datetime", "10 minutes"), "pickup_borough")`
# MAGIC puis `count(*)` + `avg("total_amount")`.
# MAGIC
# MAGIC ### Mode sortie : compromis pragmatique `append` (avec justification)
# MAGIC
# MAGIC **Idéalement** on voudrait `update` pour suivre l'évolution des KPI en
# MAGIC temps quasi-réel, y compris pour les fenêtres encore "ouvertes"
# MAGIC (event_time > watermark).
# MAGIC
# MAGIC **En pratique sur Databricks Free Edition serverless**, trois contraintes
# MAGIC bloquent `update` :
# MAGIC
# MAGIC 1. **`DELTA_UNSUPPORTED_OUTPUT_MODE`** : Delta ne supporte pas
# MAGIC    `.toTable()` en mode `update` direct. Il faut passer par
# MAGIC    `foreachBatch` + `MERGE INTO`.
# MAGIC 2. **`NOT_SUPPORTED_WITH_SERVERLESS` pour `GLOBAL TEMPORARY VIEW`** : le
# MAGIC    pattern canonique du MERGE Delta utilise une global temp view, non
# MAGIC    supportée sur compute serverless.
# MAGIC 3. **Blocage observé** : même en passant par `createOrReplaceTempView`
# MAGIC    (session-local), le `MERGE` sur la clé `(window, pickup_borough)`
# MAGIC    bloque la query (probable interaction avec les contraintes de
# MAGIC    serialisation des sessions serverless).
# MAGIC
# MAGIC **Décision pragmatique** : on bascule sur **`append`** avec watermark.
# MAGIC En mode `append`, Spark émet chaque fenêtre **une seule fois**, à
# MAGIC l'instant où le watermark fait passer le max event-time au-delà de la
# MAGIC fin de la fenêtre. C'est moins "live" qu'`update` (latence = durée
# MAGIC fenêtre + watermark = 10 min + 5 min = 15 min), mais :
# MAGIC
# MAGIC - Toutes les fenêtres complètes finissent par apparaître.
# MAGIC - Pas de risque d'inflation de l'état mémoire.
# MAGIC - Compatible avec `.toTable()` Delta direct.
# MAGIC - **Suffisant pour ce TP** (analyse a posteriori, pas dashboard live).
# MAGIC
# MAGIC **Mode `complete` reste exclu** : voir Q2 §10 (coût quadratique, I/O).
# MAGIC
# MAGIC ### Checkpoint : `/Volumes/.../checkpoints/kpi_stream/`
# MAGIC
# MAGIC Distinct des deux autres queries.

# COMMAND ----------

from pyspark.sql.functions import window, count, avg, col

def build_kpi_stream():
    """Agrégation KPI : trajets + montant moyen par borough × fenêtre 10 min."""
    df_silver = (
        spark.readStream
            .table(SILVER_STREAM_TABLE)
            .withColumn("tpep_pickup_datetime",
                        col("tpep_pickup_datetime").cast("timestamp"))
            .withWatermark("tpep_pickup_datetime", WATERMARK)
    )
    return (
        df_silver
            .groupBy(
                window(col("tpep_pickup_datetime"), "10 minutes").alias("window"),
                col("pickup_borough"),
            )
            .agg(
                count("*").alias("trip_count"),
                avg("total_amount").alias("avg_total_amount"),
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline complet : simulateur → bronze → silver → kpi
# MAGIC
# MAGIC On enchaîne 3 jobs `availableNow` par itération :
# MAGIC 1. **Simulateur** (thread daemon, 1 batch) — mode `normal`,
# MAGIC    `late_data_ratio=0.15`, rate 20.
# MAGIC 2. **Bronze** (Auto Loader cloudFiles) → `bronze_stream_taxi`.
# MAGIC 3. **Silver** (watermark + Stream-Static Join) → `silver_stream_taxi`.
# MAGIC 4. **KPI** (agrégation fenêtre) → `kpi_stream_by_window` (mode `append` —
# MAGIC    bascule `update`→`append` justifiée §5).
# MAGIC
# MAGIC On fait **2 mesures Bronze/Silver** à des moments différents (itération 5
# MAGIC et itération 10) pour observer l'évolution du ratio.

# COMMAND ----------

import sys
if VOL_SCRIPTS not in sys.path:
    sys.path.insert(0, VOL_SCRIPTS)

from taxi_simulator import TaxiStreamSimulator
from pyspark.sql.functions import current_timestamp, col

def build_bronze_stream():
    """Job Bronze repris de S1 (Auto Loader cloudFiles)."""
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

print("✓ Fonctions build_bronze_stream, build_silver_stream, build_kpi_stream définies.")

# COMMAND ----------

import time
import threading

NB_ITERATIONS = 10
PAUSE_SECONDS = 20

bronze_silver_snapshots = []   # validation Bronze/Silver à 2 moments
metrics_per_iter = []

for i in range(NB_ITERATIONS):
    print(f"\n=== Itération {i+1}/{NB_ITERATIONS} ===")

    # 1. Simulateur (mode normal + 15% de late data)
    sim = TaxiStreamSimulator(
        output_path=SIM_OUTPUT_PATH,
        rate=20,
        interval_seconds=0,
        mode="normal",
        late_data_ratio=0.15,
        duration_batches=1,
        seed=100 + i,        # seed offset par rapport à S1 pour de la variété
        verbose=False,
    )
    t = threading.Thread(target=sim.run, daemon=True)
    t.start()
    t.join()

    # 2. Job Bronze
    q_bronze = (
        build_bronze_stream().writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_BRONZE)
            .trigger(availableNow=True)
            .toTable(BRONZE_STREAM_TABLE)
    )
    q_bronze.awaitTermination()
    b_in = (q_bronze.lastProgress or {}).get("numInputRows", 0)

    # 3. Job Silver
    q_silver = (
        build_silver_stream().writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_SILVER)
            .trigger(availableNow=True)
            .toTable(SILVER_STREAM_TABLE)
    )
    q_silver.awaitTermination()
    s_in = (q_silver.lastProgress or {}).get("numInputRows", 0)

    # 4. Job KPI (mode append — fenêtres émises à la fermeture du watermark)
    q_kpi = (
        build_kpi_stream().writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_KPI)
            .trigger(availableNow=True)
            .toTable(KPI_STREAM_TABLE)
    )
    q_kpi.awaitTermination()
    k_in = (q_kpi.lastProgress or {}).get("numInputRows", 0)

    metrics_per_iter.append({
        "iter": i + 1,
        "bronze_inputRows": b_in,
        "silver_inputRows": s_in,
        "kpi_inputRows":    k_in,
    })
    print(f"  bronze={b_in}  silver={s_in}  kpi={k_in}")

    # 5. Snapshot Bronze/Silver à 2 moments (iter 5 et iter 10)
    if (i + 1) in (5, 10):
        n_b = spark.table(BRONZE_STREAM_TABLE).count()
        n_s = spark.table(SILVER_STREAM_TABLE).count()
        ratio = (n_s / n_b) if n_b else 0
        bronze_silver_snapshots.append({
            "iter": i + 1,
            "bronze_total": n_b,
            "silver_total": n_s,
            "ratio_silver_bronze": round(ratio, 4),
        })
        print(f"  📸 Snapshot iter {i+1}: bronze={n_b}, silver={n_s}, ratio={ratio:.2%}")

    if i < NB_ITERATIONS - 1:
        time.sleep(PAUSE_SECONDS)

print("\nPipeline S2 terminé.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation Bronze/Silver — évolution du ratio
# MAGIC
# MAGIC Le ratio `silver/bronze` doit refléter la perte due au watermark : on
# MAGIC attend théoriquement ~91.7% (perte ≈ 8.3% : 15% × 55.5% des retards
# MAGIC hors watermark).
# MAGIC
# MAGIC En pratique, ce ratio fluctue selon :
# MAGIC - l'ordre d'arrivée des batches (le watermark utilise le max event-time
# MAGIC   vu, qui se balade aléatoirement grâce à `_random_datetime` du
# MAGIC   simulateur) ;
# MAGIC - le timing entre l'ingestion bronze et le job silver (`availableNow`
# MAGIC   sur des fichiers fraîchement écrits).

# COMMAND ----------

import pandas as pd

df_snapshots = pd.DataFrame(bronze_silver_snapshots)
display(df_snapshots)

df_metrics = pd.DataFrame(metrics_per_iter)
display(df_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commentaire sur l'évolution
# MAGIC
# MAGIC On regarde si le ratio évolue entre l'itération 5 et l'itération 10 :
# MAGIC - **stable ~92%** → comportement attendu, watermark capture les retards
# MAGIC   dans sa fenêtre.
# MAGIC - **dégrade > 8.3%** → soit un batch a apporté beaucoup de late data
# MAGIC   très anciennes (hors watermark), soit des fluctuations dues au
# MAGIC   caractère pseudo-aléatoire du simulateur (`seed=100+i`).
# MAGIC - **proche de 100%** → le watermark est presque jamais atteint
# MAGIC   (Event Times tirés sur 24h aléatoirement, donc le max event-time se
# MAGIC   balade fortement et la zone hors watermark fluctue beaucoup).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Display KPI par fenêtre × borough
# MAGIC
# MAGIC Au moins plusieurs fenêtres et plusieurs boroughs (Manhattan, Queens,
# MAGIC Brooklyn, Bronx, Staten Island, plus EWR pour les trajets aéroport).
# MAGIC
# MAGIC Grâce à `_random_datetime` du simulateur qui tire des Event Times sur
# MAGIC les 24h de la journée courante, on obtient **plein de fenêtres
# MAGIC distinctes** dès quelques itérations (pas besoin d'attendre 15 min réel
# MAGIC pour avoir 15 min d'Event Time couvert).

# COMMAND ----------

from pyspark.sql.functions import col

df_kpi = (
    spark.table(KPI_STREAM_TABLE)
        .orderBy(col("window.start").desc(), col("pickup_borough"))
)
print(f"Total fenêtres × boroughs : {df_kpi.count()}")
display(df_kpi)

# COMMAND ----------

# Stats agrégées : nb de fenêtres par borough
from pyspark.sql.functions import countDistinct

df_kpi_summary = (
    spark.table(KPI_STREAM_TABLE)
        .groupBy("pickup_borough")
        .agg(
            countDistinct("window").alias("nb_windows"),
            count("*").alias("total_rows"),
        )
        .orderBy(col("nb_windows").desc())
)
display(df_kpi_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Question 1 — Watermark divisé par 5 (de 5 min à 1 min)
# MAGIC
# MAGIC ### Hypothèses
# MAGIC
# MAGIC - Simulateur : `late_data_ratio = 0.15`, retard uniforme sur `[1, 10]` min.
# MAGIC - Watermark actuel : **5 min** → perd les retards > 5 min.
# MAGIC - Watermark divisé par 5 : **1 min** → perd les retards > 1 min.
# MAGIC
# MAGIC ### Calcul
# MAGIC
# MAGIC La distribution uniforme `U[1, 10]` a une probabilité de tomber sous
# MAGIC `wm` égale à `P(r ≤ wm) = (wm - 1) / (10 - 1)`.
# MAGIC
# MAGIC | Watermark | P(retard ≤ wm) | P(retard > wm) | Pertes sur tous trajets |
# MAGIC |---|---|---|---|
# MAGIC | 5 min (actuel) | (5-1)/9 ≈ **44.4%** | **55.5%** | 15% × 55.5% ≈ **8.3%** |
# MAGIC | 1 min (÷5) | (1-1)/9 = **0%** | **100%** | 15% × 100% = **15.0%** |
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC Diviser le watermark par 5 (passer de 5 min à 1 min) fait passer la
# MAGIC perte de **~8.3% à 15%** des trajets totaux — soit **+6.7 points** de
# MAGIC pertes en valeur absolue, ou **~1.8× plus** de pertes en valeur relative.
# MAGIC
# MAGIC Concrètement, avec un watermark de 1 min, **toute donnée tardive est
# MAGIC perdue** (le simulateur a une borne basse de 1 min, donc aucun retard
# MAGIC n'est strictement inférieur à 1 min). C'est l'équivalent de ne pas
# MAGIC avoir de tolérance aux late data.
# MAGIC
# MAGIC ### Confrontation aux observations
# MAGIC
# MAGIC En pratique, le ratio `silver/bronze` mesuré au §7 doit être proche de
# MAGIC 91.7% (= 100% - 8.3%). S'il est plus élevé (~95-100%), c'est que le
# MAGIC simulateur n'a pas généré assez de retards "vraiment tardifs" pour
# MAGIC dépasser le watermark sur cette courte simulation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Question 2 — Pourquoi `complete` est dangereux sur `kpi_stream_by_window`
# MAGIC
# MAGIC ### Dimensionnement
# MAGIC
# MAGIC - **Fenêtres en 24 h** : 1 fenêtre toutes les 10 min → 144 fenêtres / jour.
# MAGIC - **Boroughs** observables : ~7 (Manhattan, Queens, Brooklyn, Bronx,
# MAGIC   Staten Island, EWR, + Null pour les zones sans match).
# MAGIC - **Lignes max** = 144 × 7 ≈ **1 000 lignes** dans `kpi_stream_by_window`
# MAGIC   après 24 h.
# MAGIC
# MAGIC ### Coût en mode `complete`
# MAGIC
# MAGIC - Mode `complete` = **réécrit l'intégralité** de la table résultat à
# MAGIC   chaque micro-batch.
# MAGIC - Avec un batch toutes les 20 s (notre cadence), il y a **4 320
# MAGIC   batches/24 h**.
# MAGIC - Le nombre de lignes à écrire **croît linéairement** avec le temps :
# MAGIC   `~ batch_index × (lignes_finales/4320)`.
# MAGIC - **Volume cumulé écrit** : `Σᵢ₌₁..4320 (1000 × i / 4320) ≈ 2.16 M lignes`
# MAGIC   écrites pour seulement **1 000 lignes utiles** en sortie. Soit **2 160×
# MAGIC   plus d'I/O** qu'`update`.
# MAGIC
# MAGIC ### Coût en mémoire
# MAGIC
# MAGIC - Une ligne KPI = `window` struct (2 timestamps = 16 B) + `pickup_borough`
# MAGIC   (string ~12 B) + `trip_count` (long 8 B) + `avg_total_amount` (double
# MAGIC   8 B) ≈ **44 B utiles**.
# MAGIC - Overhead JVM/Spark ≈ ×10 → **~440 B/ligne en mémoire**.
# MAGIC - Mémoire pour l'état à 24 h : `1 000 × 440 B ≈ 440 KB` — anecdotique
# MAGIC   en absolu.
# MAGIC
# MAGIC ### Le vrai danger
# MAGIC
# MAGIC Ce n'est pas la mémoire (~440 KB ≪ 15-30 Go) mais :
# MAGIC
# MAGIC 1. **L'écriture quadratique** : 2.16 M lignes écrites → I/O explose, le
# MAGIC    job ralentit ou time out.
# MAGIC 2. **Le journal Delta dégradé** : chaque batch = un commit qui réécrit
# MAGIC    tout. `VACUUM` ne libère rien tant que les versions historiques sont
# MAGIC    rétention. Le `_delta_log/` enfle.
# MAGIC 3. **L'incompatibilité avec une fenêtre temporelle non bornée** : si on
# MAGIC    laisse tourner plus de 24 h sans retention, l'état cumulé grandit
# MAGIC    sans limite (le watermark coupe les fenêtres anciennes, mais
# MAGIC    `complete` réécrit même les fenêtres déjà closes).
# MAGIC
# MAGIC ### Estimation mémoire 24 h
# MAGIC
# MAGIC **Mémoire utile** : ~440 KB en état Spark.
# MAGIC **Empreinte disque cumulée** des écritures complete : ~1 GB de fichiers
# MAGIC Delta intermédiaires avant compaction (2.16 M lignes × ~500 B en
# MAGIC Parquet/Delta avec metadata).
# MAGIC
# MAGIC ### Conclusion
# MAGIC
# MAGIC Sur compte Databricks gratuit, le mode `complete` sur `kpi_stream_by_window`
# MAGIC n'est pas un problème de **mémoire** mais d'**I/O** et de **dette Delta**.
# MAGIC C'est pour ça qu'on utilise `update`, qui n'écrit que les `(window,
# MAGIC borough)` modifiés depuis le dernier batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Arrêt explicite des streams (règle transversale)

# COMMAND ----------

for q in spark.streams.active:
    print(f"Stopping query: {q.name} (id={q.id})")
    q.stop()
print("Toutes les streams sont arrêtées.")
