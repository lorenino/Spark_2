# Databricks notebook source
# MAGIC %md
# MAGIC # S1 — Ingestion Stream (NYC Taxi)
# MAGIC
# MAGIC **Auteur** : Lorenzo Faloci — trigramme `LFA`
# MAGIC **Cours** : Spark Structured Streaming — Séance 1
# MAGIC **Date** : 2026-05-19
# MAGIC
# MAGIC ## Objectif
# MAGIC
# MAGIC Construire un premier pipeline streaming Bronze qui ingère les fichiers
# MAGIC Parquet produits par le simulateur `taxi_simulator.py` via **Auto Loader**
# MAGIC (`cloudFiles`) et les matérialise en table Delta `bronze_stream_taxi` en
# MAGIC mode `append`.
# MAGIC
# MAGIC ## Contraintes Databricks Free Edition (serverless)
# MAGIC
# MAGIC - `processingTime` lève `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.
# MAGIC - Seul trigger compatible : **`availableNow=True`**.
# MAGIC - Pour simuler le continu : boucle Python qui alterne simulateur
# MAGIC   (thread daemon) et job streaming `availableNow`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Paramètres

# COMMAND ----------

TRIGRAMME = "LFA"

CATALOG = "workspace"
SCHEMA  = f"tp_spark_{TRIGRAMME.lower()}"

# Volumes Unity Catalog (un volume par concern, convention du cours)
VOL_SCRIPTS     = f"/Volumes/{CATALOG}/{SCHEMA}/scripts"
VOL_RAW_DATA    = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"
VOL_CHECKPOINTS = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"
VOL_SCHEMAS     = f"/Volumes/{CATALOG}/{SCHEMA}/schemas"

# Chemins applicatifs
SIM_OUTPUT_PATH = f"{VOL_RAW_DATA}/stream_input/"
CHECKPOINT_PATH = f"{VOL_CHECKPOINTS}/bronze_stream/"
SCHEMA_LOCATION = f"{VOL_SCHEMAS}/bronze_stream/"

# Tables
BRONZE_STREAM_TABLE = f"{CATALOG}.{SCHEMA}.bronze_stream_taxi"
BRONZE_BATCH_TABLE  = f"{CATALOG}.{SCHEMA}.bronze_taxi_trips"  # référence Spark Core (peut être absente)

print(f"SIM_OUTPUT_PATH = {SIM_OUTPUT_PATH}")
print(f"CHECKPOINT_PATH = {CHECKPOINT_PATH}")
print(f"BRONZE_STREAM   = {BRONZE_STREAM_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bootstrap — schema + volumes Unity Catalog
# MAGIC
# MAGIC Le notebook est idempotent : on peut le rejouer sans casser l'existant.
# MAGIC On crée le schema, puis quatre volumes managés (scripts, raw_data,
# MAGIC checkpoints, schemas).

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
for vol_name in ("scripts", "raw_data", "checkpoints", "schemas"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{vol_name}")

# Sous-dossiers applicatifs (no-op si présents)
dbutils.fs.mkdirs(SIM_OUTPUT_PATH)
dbutils.fs.mkdirs(CHECKPOINT_PATH)
dbutils.fs.mkdirs(SCHEMA_LOCATION)

print(f"Schema {CATALOG}.{SCHEMA} OK")
print("Volumes : scripts, raw_data, checkpoints, schemas")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.bis — Action manuelle : déposer le simulateur
# MAGIC
# MAGIC Avant de continuer, **uploader le fichier `taxi_simulator.py`** (fourni
# MAGIC par le prof, renommé depuis `Script.py`) dans
# MAGIC `/Volumes/workspace/tp_spark_lfa/scripts/`.
# MAGIC
# MAGIC Méthode UI : Catalog → workspace → tp_spark_lfa → scripts →
# MAGIC **Upload to this volume**.

# COMMAND ----------

# Vérification que le simulateur est bien présent
import os
sim_path = f"{VOL_SCRIPTS}/taxi_simulator.py"
assert os.path.exists(sim_path), (
    f"❌ Fichier absent : {sim_path}\n"
    f"   → uploader taxi_simulator.py dans le volume scripts via Catalog UI."
)
print(f"✓ Simulateur présent : {sim_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Configuration du simulateur — justification
# MAGIC
# MAGIC Le simulateur est lancé dans un **thread daemon** (s'arrête tout seul si
# MAGIC le notebook se détache du cluster).
# MAGIC
# MAGIC | Paramètre | Valeur | Raison |
# MAGIC |---|---|---|
# MAGIC | `rate` | **20 trajets/batch** | Borne basse de la fourchette recommandée (10-50). Volume suffisant pour observer la distribution NYC Taxi sans saturer le single-node serverless. |
# MAGIC | `interval_seconds` | **0** | On gère la cadence dans la boucle Python externe (20 s entre itérations). Le sleep interne du simulateur interférerait avec `t.join()`. |
# MAGIC | `duration_batches` | **1** | Un seul batch par itération → contrôle granulaire de l'alternance sim/job. 10 itérations = 10 micro-batches Spark (≥ 5 exigés). |
# MAGIC | `mode` | **`normal`** | Imposé par le sujet (pas de chaos, pas de late). |
# MAGIC | `seed` | **`i`** | Seed variable par itération → variété sans perdre la reproductibilité. |
# MAGIC | `late_data_ratio` | **`0.0`** | Pas de retard event-time en mode normal. |
# MAGIC
# MAGIC **Total simulé** : 10 × 20 = **200 lignes**, ~3 min réel.

# COMMAND ----------

import sys
if VOL_SCRIPTS not in sys.path:
    sys.path.insert(0, VOL_SCRIPTS)

from taxi_simulator import TaxiStreamSimulator
print("✓ TaxiStreamSimulator importé.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pipeline Bronze streaming — Auto Loader + Delta
# MAGIC
# MAGIC ### Trigger : `availableNow=True`
# MAGIC
# MAGIC Le compute serverless du compte Free Edition **interdit**
# MAGIC `processingTime`. `availableNow=True` traite ce qui est disponible puis
# MAGIC s'arrête : on l'invoque 10 fois dans la boucle pour simuler le continu.
# MAGIC
# MAGIC ### Checkpoint : Volume Unity Catalog persistant
# MAGIC
# MAGIC Le checkpoint est dans un **Volume UC** (`/Volumes/.../checkpoints/`),
# MAGIC pas en DBFS ni `/tmp/`. Raisons :
# MAGIC - Survie aux redémarrages cluster (timeout 2 h compte Free).
# MAGIC - Idempotence : Auto Loader retient les fichiers déjà traités →
# MAGIC   exactly-once.
# MAGIC - Lié à *cette* query : changement de schéma → suppression manuelle.
# MAGIC
# MAGIC ### Colonnes techniques
# MAGIC
# MAGIC - `ingestion_timestamp` = `current_timestamp()` au micro-batch
# MAGIC - `source_file` = `_metadata.file_path`

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col

def build_bronze_stream():
    """DataFrame streaming Bronze depuis Auto Loader."""
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "parquet")
            .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
            .option("cloudFiles.inferColumnTypes", "true")
            .load(SIM_OUTPUT_PATH)
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
    )

# Sanity check : schéma sans démarrer le job (Auto Loader ne peut inférer
# que si des fichiers existent déjà dans SIM_OUTPUT_PATH — on tolère l'absence
# au premier run, la boucle ci-dessous produira les Parquet nécessaires).
try:
    df_stream = build_bronze_stream()
    df_stream.printSchema()
except Exception as e:
    print(f"ℹ️  Schéma non encore inférable (dossier vide) — normal au 1er run.")
    print(f"   Détail : {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Boucle simulateur + ingestion (10 itérations × 20 s)
# MAGIC
# MAGIC Chaque itération :
# MAGIC 1. Lance le simulateur en **thread daemon** (1 batch = 20 trajets).
# MAGIC 2. `t.join()` attend l'écriture du fichier Parquet.
# MAGIC 3. Job streaming `availableNow=True` consomme les nouveaux fichiers.
# MAGIC 4. `awaitTermination()` jusqu'à la fin du job.
# MAGIC 5. Pause 20 s avant l'itération suivante (sauf dernière).

# COMMAND ----------

import time
import threading

NB_ITERATIONS = 10
PAUSE_SECONDS = 20

batches_metrics = []

for i in range(NB_ITERATIONS):
    print(f"\n=== Itération {i+1}/{NB_ITERATIONS} ===")

    # 1. Simulateur en thread daemon (1 batch)
    sim = TaxiStreamSimulator(
        output_path=SIM_OUTPUT_PATH,
        rate=20,
        interval_seconds=0,
        mode="normal",
        duration_batches=1,
        seed=i,
        verbose=False,
    )
    t = threading.Thread(target=sim.run, daemon=True)
    t.start()
    t.join()
    print(f"  Simulateur OK (seed={i})")

    # 2. Job streaming availableNow
    df_stream = build_bronze_stream()
    query = (
        df_stream.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_PATH)
            .trigger(availableNow=True)
            .toTable(BRONZE_STREAM_TABLE)
    )
    query.awaitTermination()

    # 3. Métriques
    last = query.lastProgress
    if last:
        batches_metrics.append({
            "iteration": i + 1,
            "numInputRows": last.get("numInputRows", 0),
            "batchDuration_ms": last.get("batchDuration", 0),
            "inputRowsPerSec": last.get("inputRowsPerSecond", 0),
        })
        print(f"  → numInputRows={last.get('numInputRows')} "
              f"batchDuration={last.get('batchDuration')} ms")

    if i < NB_ITERATIONS - 1:
        time.sleep(PAUSE_SECONDS)

print("\nPipeline terminé.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Preuve d'exécution — métriques par itération
# MAGIC
# MAGIC `query.lastProgress` consigne les métriques de chaque appel
# MAGIC `availableNow` : `numInputRows`, `batchDuration`, `inputRowsPerSecond`.
# MAGIC Pour la trace Spark UI officielle, ouvrir **View → Spark UI → Structured
# MAGIC Streaming** dans la barre du notebook après exécution (l'historique des
# MAGIC queries contient une entrée par itération).

# COMMAND ----------

import pandas as pd
metrics_df = pd.DataFrame(batches_metrics)
display(metrics_df)

print(f"Micro-batches    : {len(metrics_df)}")
print(f"Lignes ingérées  : {metrics_df['numInputRows'].sum()}")
print(f"Durée moyenne    : {metrics_df['batchDuration_ms'].mean():.0f} ms")
print(f"Throughput moyen : {metrics_df['inputRowsPerSec'].mean():.1f} rows/sec")

# COMMAND ----------

n_bronze_stream = spark.table(BRONZE_STREAM_TABLE).count()
print(f"{BRONZE_STREAM_TABLE} contient {n_bronze_stream:,} lignes")
spark.table(BRONZE_STREAM_TABLE).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation cohérence schéma vs Spark Core
# MAGIC
# MAGIC Référence : `bronze_taxi_trips` de Spark Core S5 si présente. Sinon
# MAGIC (workspace from scratch), on compare au **schéma NYC Taxi canonique**
# MAGIC documenté par le simulateur (PDF cours).
# MAGIC
# MAGIC Différences attendues côté stream (acceptables) :
# MAGIC - `ingestion_timestamp` — ajouté par le pipeline (technique)
# MAGIC - `source_file` — ajouté par le pipeline (technique)
# MAGIC - `sim_batch_id` — injecté par le simulateur
# MAGIC - `sim_is_corrupted` — injecté par le simulateur (oracle quarantaine S3)

# COMMAND ----------

# Schéma NYC Taxi canonique (du PDF Simulateur)
NYC_TAXI_CANONICAL = {
    "VendorID":              "int",
    "tpep_pickup_datetime":  "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count":       "double",
    "trip_distance":         "double",
    "PULocationID":          "int",
    "DOLocationID":          "int",
    "payment_type":          "int",
    "fare_amount":           "double",
    "tip_amount":            "double",
    "total_amount":          "double",
}

def schema_signature(table_name):
    return {f.name: f.dataType.simpleString() for f in spark.table(table_name).schema.fields}

sig_stream = schema_signature(BRONZE_STREAM_TABLE)

# Tentative de récupérer le schéma batch (Spark Core S5)
try:
    sig_batch = schema_signature(BRONZE_BATCH_TABLE)
    reference_name = "bronze_taxi_trips (Spark Core S5)"
except Exception:
    print(f"⚠️  Table batch {BRONZE_BATCH_TABLE} absente — fallback sur le "
          f"schéma NYC Taxi canonique documenté.")
    sig_batch = NYC_TAXI_CANONICAL
    reference_name = "NYC Taxi canonique (doc simulateur)"

common      = set(sig_stream) & set(sig_batch)
only_stream = set(sig_stream) - set(sig_batch)
only_batch  = set(sig_batch)  - set(sig_stream)

print(f"\n=== Comparaison vs {reference_name} ===\n")
print(f"Colonnes communes ({len(common)}) :")
for c in sorted(common):
    same = "✓" if sig_stream[c] == sig_batch[c] else "✗"
    print(f"  {same} {c:25s}  stream={sig_stream[c]:15s} | ref={sig_batch[c]}")

print(f"\nUniquement dans bronze_stream_taxi ({len(only_stream)}) :")
for c in sorted(only_stream):
    print(f"  + {c}: {sig_stream[c]}")

print(f"\nUniquement dans la référence ({len(only_batch)}) :")
for c in sorted(only_batch):
    print(f"  - {c}: {sig_batch[c]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Commentaire
# MAGIC
# MAGIC Les divergences côté stream sont **attendues** :
# MAGIC - `ingestion_timestamp` + `source_file` : colonnes techniques ajoutées
# MAGIC   explicitement par le pipeline Bronze pour la traçabilité.
# MAGIC - `sim_batch_id` + `sim_is_corrupted` : ajoutées par le simulateur.
# MAGIC   `sim_is_corrupted` est un **oracle** (à n'utiliser qu'a posteriori en S3
# MAGIC   pour mesurer le recall de la quarantaine, jamais comme feature).
# MAGIC
# MAGIC Sur la comparaison ci-dessus, le mismatch éventuel sur les types
# MAGIC `int vs integer` est une équivalence cosmétique (`int = integer = int32`
# MAGIC dans Spark) — pas une vraie divergence. Si une **colonne canonique
# MAGIC manque** côté stream, c'est que le simulateur ne la génère pas ; à
# MAGIC corriger côté simulateur.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Question théorique — Pourquoi `complete` est impraticable pour Bronze
# MAGIC
# MAGIC Le mode `complete` impose à Spark de **réécrire intégralement la table
# MAGIC résultat à chaque micro-batch**. Sur une table Bronze qui ne fait
# MAGIC qu'accumuler des évènements, c'est catastrophique :
# MAGIC
# MAGIC 1. **Coût d'écriture quadratique.** Pour N batches de `rate` lignes,
# MAGIC    le volume cumulé écrit n'est plus N×rate (append) mais
# MAGIC    Σᵢ(i×rate) ≈ N²×rate/2. Sur mes 10 batches × 20 lignes c'est
# MAGIC    anecdotique (~1 100 lignes réécrites pour 200 ingérées), mais à
# MAGIC    1 000 batches × 1 000 lignes/batch on passe de **10⁶ lignes ingérées
# MAGIC    à ~5×10⁸ lignes écrites** — 500× plus d'I/O qu'`append`.
# MAGIC 2. **Incompatible avec l'API.** `complete` requiert une **agrégation
# MAGIC    stateful** dans la query (`groupBy`, `count`...). Une ingestion
# MAGIC    `readStream → writeStream` sans agrégation lève une
# MAGIC    `AnalysisException` — Spark refuse car il faudrait conserver tout
# MAGIC    l'historique en mémoire.
# MAGIC 3. **Pression mémoire driver.** `complete` reconstruit toute la sortie
# MAGIC    en mémoire avant écriture. Sur compute single-node serverless avec
# MAGIC    15-30 Go RAM, la table déborde dès quelques centaines de Mo.
# MAGIC 4. **Time-travel Delta dégradé.** En `append`, le journal Delta est
# MAGIC    incrémental (1 commit = N lignes ajoutées). En `complete`, chaque
# MAGIC    commit réécrit la table : les versions deviennent des snapshots
# MAGIC    redondants, `VACUUM` ne libère rien tant qu'on n'expire pas les
# MAGIC    versions, et le time-travel perd son intérêt analytique.
# MAGIC 5. **Anti-pattern Medallion.** Bronze = source de vérité brute,
# MAGIC    *append-only par définition*. Réécrire le Bronze à chaque micro-batch
# MAGIC    contredit la garantie d'immutabilité qui justifie son existence.
# MAGIC
# MAGIC En résumé : `complete` est conçu pour des **résultats agrégés bornés**
# MAGIC (top-N, leaderboards, dashboards live), pas pour des **flux
# MAGIC d'évènements append-only**. Pour Bronze, c'est toujours `append`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Arrêt explicite des streams (règle transversale du cours)

# COMMAND ----------

for q in spark.streams.active:
    print(f"Stopping query: {q.name} (id={q.id})")
    q.stop()
print("Toutes les streams sont arrêtées.")
