# Databricks notebook source
# MAGIC %md
# MAGIC # S3 — Qualité en flux et détection d'anomalies
# MAGIC
# MAGIC **Auteur** : Lorenzo Faloci — trigramme `LFA`
# MAGIC **Cours** : Spark Structured Streaming — Séance 3
# MAGIC **Date** : 2026-05-19
# MAGIC
# MAGIC ## Objectif
# MAGIC
# MAGIC À partir de `silver_stream_taxi` (alimentée par S2), produire deux tables
# MAGIC Delta distinctes via routage par `quality_flag` :
# MAGIC
# MAGIC - **`gold_stream_taxi`** : lignes valides
# MAGIC - **`quarantine_stream_taxi`** : lignes rejetées
# MAGIC
# MAGIC Le pipeline **ne doit jamais crasher** sur donnée corrompue : routage
# MAGIC via `when/otherwise`, jamais d'exception levée.
# MAGIC
# MAGIC ## Paramètre imposé
# MAGIC
# MAGIC - Simulateur : **`mode="chaos"`** + **`chaos_ratio=0.20`** → 20% des
# MAGIC   trajets injectent une corruption parmi 6 types.
# MAGIC
# MAGIC ## Critères de rejet
# MAGIC
# MAGIC | quality_flag | Condition | Source corruption |
# MAGIC |---|---|---|
# MAGIC | `MONTANT_NEGATIF` | `total_amount < 0` ou `fare_amount < 0` | `_negative_amount` |
# MAGIC | `DISTANCE_NULLE` | `trip_distance <= 0` | `_zero_distance` |
# MAGIC | `DISTANCE_ABERRANTE` | `trip_distance > 100` (miles) | `_impossible_distance` (9999) |
# MAGIC | `LOCATION_NULLE` | `PULocationID IS NULL` | `_null_location` |
# MAGIC | `INVERSION_TEMPS` | `dropoff < pickup` | `_inverted_timestamps` (bonus, non couvert par flags standards) |
# MAGIC | `ANOMALIE_STATISTIQUE` | `\|z_score(total_amount)\| > 3` | `_huge_amount` (z-score sur montant) |
# MAGIC | `VALIDE` | aucune des conditions ci-dessus | — |
# MAGIC
# MAGIC Note : `_impossible_distance` (9999 miles) n'aurait pas été capturé
# MAGIC par `ANOMALIE_STATISTIQUE` car le z-score porte sur `total_amount`, pas
# MAGIC sur `trip_distance`. On ajoute donc un seuil dédié à 100 miles (au-delà,
# MAGIC c'est physiquement impossible pour un trajet taxi intra-NYC, même
# MAGIC vers les aéroports de banlieue).
# MAGIC
# MAGIC Le **z-score** est calculé par borough via Stream-Static Join avec une
# MAGIC table batch de référence (`ref_borough_stats`). Si le borough est null
# MAGIC ou absent de la référence, on **renvoie VALIDE** (pas d'anomalie
# MAGIC statistique inférable, on évite les `NaN` qui invalideraient le flag).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Paramètres

# COMMAND ----------

TRIGRAMME = "LFA"
CATALOG = "workspace"
SCHEMA  = f"tp_spark_{TRIGRAMME.lower()}"

# Volumes UC (créés en S1/S2, idempotent)
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
SCHEMA_LOC_BRONZE      = f"{VOL_SCHEMAS}/bronze_stream/"

# Tables
BRONZE_STREAM_TABLE    = f"{CATALOG}.{SCHEMA}.bronze_stream_taxi"
SILVER_STREAM_TABLE    = f"{CATALOG}.{SCHEMA}.silver_stream_taxi"
GOLD_STREAM_TABLE      = f"{CATALOG}.{SCHEMA}.gold_stream_taxi"
QUARANTINE_TABLE       = f"{CATALOG}.{SCHEMA}.quarantine_stream_taxi"
REF_ZONES_TABLE        = f"{CATALOG}.{SCHEMA}.ref_taxi_zones"
REF_BOROUGH_STATS      = f"{CATALOG}.{SCHEMA}.ref_borough_stats"

print(f"GOLD       = {GOLD_STREAM_TABLE}")
print(f"QUARANTINE = {QUARANTINE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bootstrap idempotent + reset checkpoints S3
# MAGIC
# MAGIC Schema + volumes existent déjà (S1/S2). On crée juste les checkpoints
# MAGIC Gold/Quarantaine et on reset les tables S3 pour repartir propre (le test
# MAGIC de résilience suppose un état initial connu).

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
for vol_name in ("scripts", "raw_data", "checkpoints", "schemas"):
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{vol_name}")

dbutils.fs.mkdirs(SIM_OUTPUT_PATH)
dbutils.fs.mkdirs(CHECKPOINT_BRONZE)
dbutils.fs.mkdirs(CHECKPOINT_SILVER)
dbutils.fs.mkdirs(CHECKPOINT_GOLD)
dbutils.fs.mkdirs(CHECKPOINT_QUARANTINE)
dbutils.fs.mkdirs(SCHEMA_LOC_BRONZE)

# Reset Gold/Quarantine pour démarrer S3 propre (tests résilience valides).
# Bronze + Silver sont conservés (continuent à recevoir les nouveaux trajets).
for tbl in (GOLD_STREAM_TABLE, QUARANTINE_TABLE):
    spark.sql(f"DROP TABLE IF EXISTS {tbl}")
for cp in (CHECKPOINT_GOLD, CHECKPOINT_QUARANTINE):
    try:
        dbutils.fs.rm(cp, recurse=True)
    except Exception:
        pass
    dbutils.fs.mkdirs(cp)

print("OK : checkpoints Gold + Quarantine reset, tables S3 vidées.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Table de référence batch — stats par borough
# MAGIC
# MAGIC Le PDF mentionne "votre table Gold de Spark Core S3 pour les stats par
# MAGIC borough". Comme la table Spark Core n'est pas disponible côté
# MAGIC `tp_spark_lfa`, on **calcule les stats à partir de `silver_stream_taxi`**
# MAGIC en lecture batch (snapshot à l'instant T). C'est une approximation
# MAGIC raisonnable : `silver_stream_taxi` contient les trajets propres de S2.
# MAGIC
# MAGIC On filtre les valeurs aberrantes (montants négatifs / extrêmes) **avant**
# MAGIC de calculer les stats, pour ne pas que les corruptions du chaos polluent
# MAGIC la référence. Cette logique est cohérente avec ce que ferait un job
# MAGIC batch Spark Core de référence.

# COMMAND ----------

from pyspark.sql.functions import avg, stddev, col, count

df_silver_snapshot = spark.table(SILVER_STREAM_TABLE).filter(
    (col("total_amount") > 0) &           # exclut MONTANT_NEGATIF
    (col("total_amount") < 500) &          # exclut MONTANT_ABERRANT
    (col("trip_distance") > 0) &           # exclut DISTANCE_NULLE
    (col("trip_distance") < 100) &         # exclut DISTANCE_IMPOSSIBLE
    col("pickup_borough").isNotNull()
)

ref_stats = (
    df_silver_snapshot
        .groupBy("pickup_borough")
        .agg(
            avg("total_amount").alias("mean_total"),
            stddev("total_amount").alias("std_total"),
            count("*").alias("n_samples"),
        )
)

# Sauvegarder en Delta pour le Stream-Static Join
ref_stats.write.format("delta").mode("overwrite").saveAsTable(REF_BOROUGH_STATS)

print(f"✓ Table {REF_BOROUGH_STATS} créée.")
spark.table(REF_BOROUGH_STATS).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pipeline qualité — routage via `when/otherwise`
# MAGIC
# MAGIC ### Approche correcte (PDF §1)
# MAGIC
# MAGIC > Ajouter une colonne de flag via des conditions `when/otherwise` sur le
# MAGIC > DataFrame, puis filtrer en deux DataFrames distincts (valide et
# MAGIC > quarantaine). **Aucune exception n'est levée** : chaque ligne est
# MAGIC > simplement routée.
# MAGIC
# MAGIC ### Gestion du z-score et des nulls
# MAGIC
# MAGIC Le z-score `(total - mean) / std` produit `NaN` si :
# MAGIC - `pickup_borough` est null (pas de match dans `ref_borough_stats`)
# MAGIC - `std_total` est null ou 0 (borough avec un seul échantillon)
# MAGIC
# MAGIC On gère ces cas en **renvoyant VALIDE** quand le z-score n'est pas
# MAGIC calculable. C'est plus prudent que de rejeter par défaut (on garde la
# MAGIC donnée brute, l'analyste pourra la qualifier plus tard).
# MAGIC
# MAGIC ### Ordre d'évaluation des `when`
# MAGIC
# MAGIC Les conditions sont évaluées **dans l'ordre** : la première qui matche
# MAGIC l'emporte. On met les anomalies évidentes (MONTANT_NEGATIF,
# MAGIC DISTANCE_NULLE...) en premier, l'ANOMALIE_STATISTIQUE en dernier. Un
# MAGIC trajet à -50$ ET avec un z-score élevé sera classé MONTANT_NEGATIF (plus
# MAGIC précis pour l'analyse de causes).

# COMMAND ----------

from pyspark.sql.functions import (
    col, when, lit, broadcast, abs as F_abs, isnan, isnull
)

WATERMARK = "5 minutes"

def build_silver_with_quality():
    """Stream Silver enrichi de la colonne quality_flag."""
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
        col("mean_total"),
        col("std_total"),
    )

    df_with_stats = df_silver.join(
        broadcast(df_stats),
        df_silver.pickup_borough == df_stats.borough_join,
        "left",
    ).drop("borough_join")

    # z-score sécurisé : NaN/null si std absent ou nul → renvoyer 0 pour ne
    # pas déclencher d'ANOMALIE_STATISTIQUE.
    df_with_z = df_with_stats.withColumn(
        "z_score",
        when(
            col("std_total").isNotNull() & (col("std_total") > 0),
            (col("total_amount") - col("mean_total")) / col("std_total"),
        ).otherwise(lit(0.0))
    )

    df_flagged = df_with_z.withColumn(
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

    return df_flagged

# Sanity check : afficher le schéma
df_test = build_silver_with_quality()
df_test.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Boucle simulateur chaos + 2 jobs (Gold + Quarantaine)
# MAGIC
# MAGIC ### Configuration simulateur
# MAGIC
# MAGIC - `mode="chaos"` + `chaos_ratio=0.20` → 20% de corruptions injectées
# MAGIC - `rate=20` (cohérent avec S1/S2)
# MAGIC - `duration_batches=1` (un batch par itération)
# MAGIC - `seed=200+i` (offset par rapport à S1/S2 pour variété)
# MAGIC
# MAGIC ### Architecture des jobs
# MAGIC
# MAGIC Chaque itération de la boucle externe :
# MAGIC 1. Simulateur → fichier Parquet
# MAGIC 2. Job Bronze (lit Parquet) → `bronze_stream_taxi`
# MAGIC 3. Job Silver (lit Bronze) → `silver_stream_taxi`
# MAGIC 4. Job Gold (lit Silver, filtre `quality_flag = VALIDE`) → `gold_stream_taxi`
# MAGIC 5. Job Quarantaine (lit Silver, filtre `quality_flag != VALIDE`) → `quarantine_stream_taxi`
# MAGIC
# MAGIC **2 checkpoints distincts** pour Gold et Quarantaine (règle PDF).

# COMMAND ----------

import sys, time, threading
if VOL_SCRIPTS not in sys.path:
    sys.path.insert(0, VOL_SCRIPTS)

from taxi_simulator import TaxiStreamSimulator
from pyspark.sql.functions import current_timestamp, col

def build_bronze_stream():
    """Job Bronze (Auto Loader cloudFiles)."""
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

def build_silver_stream():
    """Job Silver (typage + watermark + Stream-Static Join zones)."""
    from pyspark.sql.functions import broadcast
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

print("✓ Fonctions build_bronze, build_silver, build_silver_with_quality définies.")

# COMMAND ----------

NB_ITERATIONS = 10
PAUSE_SECONDS = 20

quality_per_iter = []

for i in range(NB_ITERATIONS):
    print(f"\n=== Itération {i+1}/{NB_ITERATIONS} ===")

    # 1. Simulateur chaos
    sim = TaxiStreamSimulator(
        output_path=SIM_OUTPUT_PATH,
        rate=20,
        interval_seconds=0,
        mode="chaos",
        chaos_ratio=0.20,
        duration_batches=1,
        seed=200 + i,
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

    # 4. Job Gold (lignes valides)
    q_gold = (
        build_silver_with_quality()
            .filter(col("quality_flag") == "VALIDE")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_GOLD)
            .trigger(availableNow=True)
            .toTable(GOLD_STREAM_TABLE)
    )
    q_gold.awaitTermination()
    g_in = (q_gold.lastProgress or {}).get("numInputRows", 0)

    # 5. Job Quarantaine (lignes rejetées)
    q_qrt = (
        build_silver_with_quality()
            .filter(col("quality_flag") != "VALIDE")
            .writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", CHECKPOINT_QUARANTINE)
            .trigger(availableNow=True)
            .toTable(QUARANTINE_TABLE)
    )
    q_qrt.awaitTermination()
    q_in = (q_qrt.lastProgress or {}).get("numInputRows", 0)

    quality_per_iter.append({"iter": i+1, "gold_inputRows": g_in, "qrt_inputRows": q_in})
    print(f"  gold_in={g_in}  quarantine_in={q_in}")

    if i < NB_ITERATIONS - 1:
        time.sleep(PAUSE_SECONDS)

print("\nPipeline S3 terminé.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Dashboard qualité — Moment 1 (mi-pipeline)
# MAGIC
# MAGIC On affiche le taux de rejet global et la distribution des types
# MAGIC d'anomalies à un premier moment, **idéalement à mi-parcours** (en
# MAGIC pratique : juste après la boucle). Le second relevé sera en §7 après le
# MAGIC test de résilience.

# COMMAND ----------

from pyspark.sql.functions import count, when, lit

def quality_dashboard(label):
    """Affiche taux de rejet global + distribution par flag."""
    n_gold = spark.table(GOLD_STREAM_TABLE).count()
    n_qrt  = spark.table(QUARANTINE_TABLE).count()
    n_tot  = n_gold + n_qrt
    rate   = (n_qrt / n_tot * 100) if n_tot else 0

    print(f"\n=== Dashboard qualité [{label}] ===")
    print(f"  Lignes valides    : {n_gold:,}")
    print(f"  Lignes rejetées   : {n_qrt:,}")
    print(f"  Total observé     : {n_tot:,}")
    print(f"  Taux de rejet     : {rate:.2f}%  (théorique chaos = 20%)")

    print(f"\n  Distribution par type d'anomalie :")
    dist = (
        spark.table(QUARANTINE_TABLE)
            .groupBy("quality_flag")
            .agg(count("*").alias("n"))
            .orderBy(col("n").desc())
    )
    dist.show(truncate=False)

    return {"label": label, "gold": n_gold, "quarantine": n_qrt, "rate_pct": round(rate, 2)}

snapshot_1 = quality_dashboard("Moment 1 — fin boucle")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Test de résilience (ÉLIMINATOIRE)
# MAGIC
# MAGIC ### Mécanisme garanti par Spark + Delta
# MAGIC
# MAGIC L'**exactly-once** sur le sink Delta est garanti par 2 verrous :
# MAGIC
# MAGIC 1. **Le checkpoint** mémorise l'offset lu côté source (table Silver) :
# MAGIC    `silver_stream_taxi` versions Delta `vN` traitées. Au redémarrage,
# MAGIC    Spark reprend à `vN+1`, jamais à `vN`.
# MAGIC 2. **L'écriture Delta atomique** : si le cluster crash entre l'écriture
# MAGIC    des fichiers Parquet et le commit dans `_delta_log`, Delta ignore
# MAGIC    les fichiers orphelins. Le micro-batch est rejoué proprement, **sans
# MAGIC    duplication** dans la table cible.
# MAGIC
# MAGIC ### Test
# MAGIC
# MAGIC 1. Lire `count(gold_stream_taxi)` avant restart.
# MAGIC 2. Relancer le job Gold avec le **même checkpoint** (simule un crash
# MAGIC    + redémarrage).
# MAGIC 3. Lire `count(gold_stream_taxi)` après restart.
# MAGIC 4. **Assertion** : counts identiques (aucune ligne dupliquée).

# COMMAND ----------

count_before = spark.table(GOLD_STREAM_TABLE).count()
print(f"📊 Count Gold AVANT restart : {count_before:,}")

# Relance forcée du job Gold sur le même checkpoint
# (équivaut à un restart après crash : Spark consulte le checkpoint,
# voit que tout a déjà été traité, et n'écrit rien de plus.)
q_restart = (
    build_silver_with_quality()
        .filter(col("quality_flag") == "VALIDE")
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_GOLD)
        .trigger(availableNow=True)
        .toTable(GOLD_STREAM_TABLE)
)
q_restart.awaitTermination()

count_after = spark.table(GOLD_STREAM_TABLE).count()
print(f"📊 Count Gold APRÈS restart : {count_after:,}")

if count_before == count_after:
    print(f"\n✅ EXACTLY-ONCE VALIDÉ : aucune duplication ({count_before} → {count_after})")
    print("   Mécanisme : checkpoint Spark (offset Silver) + écriture atomique Delta (_delta_log).")
else:
    diff = count_after - count_before
    print(f"\n⚠️  DUPLICATION DÉTECTÉE : +{diff} lignes après restart")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Dashboard qualité — Moment 2 (post-restart)

# COMMAND ----------

snapshot_2 = quality_dashboard("Moment 2 — post-restart")

print(f"\n📈 Évolution entre les 2 relevés :")
print(f"  Gold : {snapshot_1['gold']} → {snapshot_2['gold']} (Δ={snapshot_2['gold']-snapshot_1['gold']})")
print(f"  Quarantine : {snapshot_1['quarantine']} → {snapshot_2['quarantine']} (Δ={snapshot_2['quarantine']-snapshot_1['quarantine']})")
print(f"  Taux rejet : {snapshot_1['rate_pct']}% → {snapshot_2['rate_pct']}%")
print(f"\n  Commentaire : si Δ = 0 partout, le restart n'a rien dupliqué (cf §7).")
print(f"  Le taux de rejet observé doit être proche de 20% (chaos_ratio) si les flags")
print(f"  couvrent bien toutes les 6 types de corruption injectés.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pourquoi le taux observé est inférieur à 20 % théorique
# MAGIC
# MAGIC Le `chaos_ratio = 0.20` s'applique **uniquement aux trajets injectés
# MAGIC pendant les itérations S3** (NB_ITERATIONS × rate ≈ 200 trajets corrompus
# MAGIC à 20 %). Mais le pipeline lit `silver_stream_taxi` qui contient **aussi
# MAGIC tout l'historique propre des séances S1/S2** (mode `normal`, sans
# MAGIC corruption).
# MAGIC
# MAGIC Calcul de dilution :
# MAGIC
# MAGIC ```
# MAGIC taux_obs ≈ (n_chaos × 0.20) / (n_chaos + n_normal_S1_S2)
# MAGIC ```
# MAGIC
# MAGIC Avec ~200 trajets chaos (20 % corrompus → ~40 anomalies) et ~1 000
# MAGIC trajets propres S1/S2 déjà présents, on attend
# MAGIC `40 / 1200 ≈ 3.3 %` à `40 / 600 ≈ 6.7 %` selon le volume cumulé exact
# MAGIC d'historique. Le taux observé (~6.95 %) est **cohérent** avec cette
# MAGIC dilution.
# MAGIC
# MAGIC **Pour valider la couverture des 6 flags** indépendamment de la
# MAGIC dilution, on regarde la distribution par type (§6 dashboard) : les 6
# MAGIC catégories doivent apparaître non-vides, ce qui prouve que le routage
# MAGIC `when/otherwise` fonctionne sur chaque type de corruption.
# MAGIC
# MAGIC **Alternative pour mesurer le taux réel** : reset `gold_stream_taxi` et
# MAGIC `quarantine_stream_taxi` au début de S3 (déjà fait §2) **et** filtrer
# MAGIC le calcul du taux sur les seuls trajets injectés depuis le début de S3
# MAGIC (timestamp d'ingestion > t_start_S3). On retomberait alors sur ~20 %.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Question 1 — Variabilité de `ANOMALIE_STATISTIQUE`
# MAGIC
# MAGIC > Le taux de rejet dû à `ANOMALIE_STATISTIQUE` varie fortement d'un
# MAGIC > micro-batch à l'autre. Proposez une explication. Quel ajustement
# MAGIC > permettrait de stabiliser ce taux ?
# MAGIC
# MAGIC ### Explication
# MAGIC
# MAGIC Deux causes additives expliquent la variabilité :
# MAGIC
# MAGIC 1. **Taille d'échantillon faible par batch** : avec `rate=20` trajets
# MAGIC    par micro-batch et `chaos_ratio=0.20`, on attend ~4 corruptions par
# MAGIC    batch, soit ~0.67 par type de corruption (6 types). Le simulateur
# MAGIC    tire chaque corruption **uniformément** (`self._corruptions[int(self.rng.integers(len(self._corruptions)))]`)
# MAGIC    donc certains batches auront 0 `_huge_amount`, d'autres 2 — la
# MAGIC    variance est très haute relativement à la moyenne.
# MAGIC 2. **Stats de référence figées** : `ref_borough_stats` est calculée
# MAGIC    une seule fois (Stream-Static), donc le z-score utilise toujours
# MAGIC    les mêmes `mean`/`std`. Si un batch tombe sur des trajets
# MAGIC    "normaux" pour le borough X (z-score < 3 mais qui auraient été
# MAGIC    flagués avec un seuil 2.5), le batch suivant peut tomber sur du
# MAGIC    `_huge_amount` clair (z-score >> 3). Le taux observé saute.
# MAGIC
# MAGIC ### Ajustement implémentable
# MAGIC
# MAGIC Trois options par ordre de coût croissant :
# MAGIC
# MAGIC 1. **Augmenter le `rate`** (50-100 trajets/batch) pour réduire la
# MAGIC    variance par la loi des grands nombres. Limite : RAM Free Edition.
# MAGIC 2. **Lisser sur N batches glissants** : calculer le taux ANOMALIE_STATISTIQUE
# MAGIC    sur les 5 derniers batches au lieu du dernier seul. Smoothing
# MAGIC    exponentiel ou moyenne glissante. Implémentation : ajouter
# MAGIC    un job streaming qui agrège `count(quarantine WHERE flag = 'ANOMALIE_STATISTIQUE')`
# MAGIC    sur une fenêtre glissante de N×interval, et exposer cette série.
# MAGIC 3. **Recalibrer `ref_borough_stats` périodiquement** (par exemple toutes
# MAGIC    les heures via un job batch) pour absorber les drifts saisonniers.
# MAGIC    Ça stabilise les stats à long terme mais pas la variance batch-à-batch.
# MAGIC
# MAGIC La **#2 est la plus efficace** pour le critère "stabiliser le taux
# MAGIC observé" sans changer la sémantique du flag.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Question 2 — Ignorer définitivement vs réinjecter
# MAGIC
# MAGIC > Ignorer définitivement les données en quarantaine ou les réinjecter
# MAGIC > après correction : quels sont les arguments pour chaque option dans
# MAGIC > le contexte taxi ? Quelle serait votre décision et pourquoi ?
# MAGIC
# MAGIC ### Arguments pour **ignorer** (jeter définitivement)
# MAGIC
# MAGIC - **Simplicité opérationnelle** : pas de pipeline de correction, pas
# MAGIC   de table intermédiaire à maintenir. Le coût en code et compute est
# MAGIC   nul après le routage initial.
# MAGIC - **Traçabilité préservée** : `quarantine_stream_taxi` reste
# MAGIC   disponible pour audit, analyse a posteriori, debugging du
# MAGIC   simulateur ou du capteur amont. Rien n'est physiquement perdu, juste
# MAGIC   pas réinjecté dans le flux principal.
# MAGIC - **Évite le biais** : réinjecter des données "corrigées" peut masquer
# MAGIC   un vrai problème système (capteur défaillant, fraude au comptage).
# MAGIC   Si la flotte rapporte beaucoup de `DISTANCE_NULLE`, c'est un signal
# MAGIC   métier, pas du bruit à filtrer.
# MAGIC
# MAGIC ### Arguments pour **réinjecter après correction**
# MAGIC
# MAGIC - **Préservation du volume** : 20% de trajets perdus = 20% de revenus
# MAGIC   non comptabilisés dans les KPI. À l'échelle d'une flotte taxi, c'est
# MAGIC   massif financièrement.
# MAGIC - **Corrections évidentes** sont peu risquées :
# MAGIC   - `MONTANT_NEGATIF` → `abs(total_amount)` si on suppose erreur de
# MAGIC     signe technique (rare mais existe).
# MAGIC   - `INVERSION_TEMPS` → permuter `pickup`/`dropoff` si la durée résultante
# MAGIC     est plausible.
# MAGIC - **Couverture statistique** : ne pas perdre les trajets longue distance
# MAGIC   légitimes (course aéroport) qui sont parfois flagués `ANOMALIE_STATISTIQUE`
# MAGIC   par excès de prudence du z-score.
# MAGIC
# MAGIC ### Ma décision pour le contexte taxi
# MAGIC
# MAGIC **Approche hybride par catégorie** :
# MAGIC
# MAGIC | Type | Décision | Raison |
# MAGIC |---|---|---|
# MAGIC | `MONTANT_NEGATIF` | **Ignorer** | Signal d'un bug taxi/POS, à investiguer, pas à corriger automatiquement. |
# MAGIC | `DISTANCE_NULLE` | **Ignorer** | Course annulée ou fraude — à exclure du chiffre d'affaires officiel. |
# MAGIC | `LOCATION_NULLE` | **Ignorer** | Donnée incomplète, GPS HS — pas réparable a posteriori. |
# MAGIC | `INVERSION_TEMPS` | **Réinjecter** après correction | Erreur d'horodatage triviale à corriger (permutation). Trajet réel sous-jacent. |
# MAGIC | `ANOMALIE_STATISTIQUE` | **Réinjecter** avec flag | Probable trajet légitime mais inhabituel (long courrier aéroport, surge nuit). Garder mais signaler dans `gold` avec une colonne `is_outlier`. |
# MAGIC
# MAGIC En pratique on aurait 2 tables Gold : `gold_clean` (sans outliers) et
# MAGIC `gold_with_outliers` (avec, pour le revenue brut). La quarantaine ne
# MAGIC contient au final que les **vraies erreurs techniques**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Arrêt explicite des streams

# COMMAND ----------

for q in spark.streams.active:
    print(f"Stopping query: {q.name} (id={q.id})")
    q.stop()
print("Toutes les streams sont arrêtées.")
