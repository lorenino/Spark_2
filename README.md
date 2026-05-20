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
- **5 contraintes Databricks Free Edition serverless rencontrées et documentées** :
  1. `processingTime` non supporté → boucle Python avec `availableNow=True`
  2. `update` Delta refusé sur `.toTable()` → bascule `append` documentée
  3. `GLOBAL TEMPORARY VIEW` interdit serverless
  4. `pyspark.ml` bloqué Py4J → bascule Stream-Static Join sur table déjà clusterisée
  5. `CLEAR CACHE` (`spark.catalog.clearCache()`) bloqué serverless → wrap try/except dans S5 Partie B (la mesure cache/no-cache reste valide via le premier run baseline)

## Honnêteté technique

Section dédiée dans le notebook S5 (`C.3 — Ce qui ne fonctionne pas`) : 11 points
recensant simplifications, limites observées et points qu'on ferait différemment en
production. Détail dans S5.

## Workspace Databricks

- Workspace : `dbc-422021d7-64a4.cloud.databricks.com` (Free Edition serverless)
- Catalog : `workspace`, schema : `tp_spark_lfa`
- Compte étudiant : `l.faloci@myskolae.fr`

## Notes correcteur — corrections post-rendu

Une revue technique du livrable a été conduite après la première mise en
ligne. Les **sources Python `.py`** dans `sources/` reflètent la version
**finale post-revue**. Les `.dbc` contiennent les **outputs des runs Databricks
originaux** (preuve d'exécution). Différences entre les deux :

| Séance | Correction | Fichier source | Présent dans `.dbc` ? |
|---|---|---|---|
| **S2** | En-tête tableau "Mode sortie KPI = `update`" → `append` (cible `update`, justification §5 inchangée) | `sources/S2_Silver_Stream_LFA.py` ligne 29 | non — modif cosmétique markdown |
| **S3** | Ajout cellule Markdown explicite expliquant la **dilution** du taux de rejet (≈ 6.95 % observé vs 20 % théorique) par l'historique propre S1/S2 dans `silver_stream_taxi` | `sources/S3_Qualite_Stream_LFA.py` §8bis | non — ajout cellule markdown |
| **S4** | `LABEL_MAP` corrigé selon les centroïdes Spark Core S5 : `0=BANLIEUE_REG, 1=LONG_COURRIER, 2=HUB_URBAIN, 3=PREMIUM_LUXE` (initialement `URBAIN_CENTRE/HUB_VOLUME/BANLIEUE_GREEN/LONG_COURRIER` inversé sur les clusters 0 et 2) | `sources/S4_Scoring_Stream_LFA.py` lignes 268-274 | non — code corrigé mais pipeline pas re-runné |
| **S5** | `nb_iter` du benchmark passé de **5 à 10** itérations par rate (conformément au PDF Séance 5 §1-2) | `sources/S5_Final_LFA.py` lignes 85-86 + 139 | non — bench effectué initialement à 5 iter pour économiser compute serverless saturé |

**Honnêteté technique sur la non-conformité S5 → 10 iter** : le bench initial
des 4 rates × 5 iter a montré que la **durée moyenne par run est dominée par
le cold start Spark** (~9 s overhead fixe, indépendant du rate). Passer à 10
iter ne change pas la nature du résultat (pas de saturation observée
{20, 50, 100, 200} sur Free Edition serverless) — voir analyse en S5 §A.4.

**Justification S4 — `transform()` vs Stream-Static Join** : la sandbox Py4J
de Databricks Free Edition serverless **bloque l'instanciation directe des
classes MLlib** (`Py4JSecurityException: VectorAssembler constructor is not
whitelisted`). Impossible donc d'utiliser `PipelineModel.load() + transform()`
comme demandé par le PDF Séance 4. Le **Stream-Static Join** sur la table
déjà clusterisée (`workspace.default.gold_quartiers_clustered` produite par
le notebook Spark Core S5 `04_ML_Clustering`) est **sémantiquement équivalent**
au scoring batch : le mapping zone → cluster est figé une fois le modèle
entraîné, donc l'inférence streaming se réduit à un lookup broadcast (~265
zones). En production sur cluster classique, on retomberait sur l'API MLflow
standard.
