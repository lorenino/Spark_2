# Spark Structured Streaming — Rendu LFA

**Auteur** : Lorenzo Faloci — trigramme `LFA`
**Cours** : ESGI M1 — Spark Structured Streaming (owner : Louis Reynouard)
**Date de rendu** : 2026-05-20
**Soutenance** : jeudi 21 mai 2026

## Livrables

Cinq notebooks `.dbc` Databricks, un par séance :

| Fichier | Séance | Sujet | Points |
|---|---|---|---|
| `S1_Ingestion_Stream_LFA.dbc` | S1 | Auto Loader + Bronze | 10 |
| `S2_Silver_Stream_LFA.dbc` | S2 | Watermark + Stream-Static Join + KPI | 15 |
| `S3_Qualite_Stream_LFA.dbc` | S3 | Quality flag + test résilience éliminatoire | 20 |
| `S4_Scoring_Stream_LFA.dbc` | S4 | Scoring K-Means (Stream-Static Join sur `gold_quartiers_clustered`) + drift monitor | 20 |
| `S5_Final_LFA.dbc` | S5 | Challenge volume + 2 optimisations + rapport | 35 |

Un bundle `TP_Spark_folder.dbc` est aussi fourni — il contient les 5 notebooks
streaming **et** les 5 notebooks Spark Core précédents (`01_Bronze` à
`05_Performance`) pour montrer la continuité entre les deux modules.

## Pipeline produit

Architecture Medallion streaming sur Databricks Free Edition serverless :

```
Simulateur (taxi_simulator.py, thread daemon)
    ↓ Parquet files
Auto Loader (cloudFiles)
    ↓
bronze_stream_taxi (Delta append)
    ↓ cast timestamp + watermark 5min + Stream-Static Join ref_taxi_zones
silver_stream_taxi (Delta append)
    ↓
    ├── quality_flag via when/otherwise → gold_stream_taxi + quarantine_stream_taxi
    ├── window 10min × borough → kpi_stream_by_window
    └── Stream-Static Join gold_quartiers_clustered (Spark Core S5) → classified_stream_taxi + drift_monitor
```

6 checkpoints isolés dans `/Volumes/workspace/tp_spark_lfa/checkpoints/`, exactly-once
garanti par Delta `_delta_log` + checkpoint Spark.

## Faits notables

- **Test de résilience S3 (éliminatoire) validé** : count Gold avant = après = 1 098, Δ=0.
- **Modèle K-Means Spark Core S5 réutilisé** via Stream-Static Join sur
  `workspace.default.gold_quartiers_clustered` (262 zones, 4 clusters).
- **4 contraintes Databricks Free Edition serverless rencontrées et documentées** :
  1. `processingTime` non supporté → boucle Python avec `availableNow=True`
  2. `update` Delta refusé sur `.toTable()` → bascule `append` documentée
  3. `GLOBAL TEMPORARY VIEW` interdit serverless
  4. `pyspark.ml` bloqué Py4J → bascule Stream-Static Join sur table déjà clusterisée

## Honnêteté technique

Section dédiée dans le notebook S5 (`C.3 — Ce qui ne fonctionne pas`) : 11 points
recensant simplifications, limites observées et points qu'on ferait différemment en
production. Détail dans S5.

## Workspace Databricks

- Workspace : `dbc-422021d7-64a4.cloud.databricks.com` (Free Edition serverless)
- Catalog : `workspace`, schema : `tp_spark_lfa`
- Compte étudiant : `l.faloci@myskolae.fr`
