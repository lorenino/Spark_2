"""Patche les .dbc S2/S3/S4/S5 avec les markdowns phase 2 (enrichissements
post validation cross-schema reelle S1 du 20/05).

Le format DBC = ZIP contenant un fichier .python (JSON NotebookV1) avec
un tableau `commands` ; chaque cellule markdown a un `command` qui commence
par `%md\n`. On patche en remplacement de phrases-cles uniques.
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

RENDU = Path(__file__).parent

# Format: (titre_section_unique, ancienne_phrase, nouvelle_phrase_complete)
# Le matching se fait sur substring exact du `command` (qui inclut "%md\n" en prefixe).

PATCHES = {
    "S2_Silver_Stream_LFA.dbc": [
        (
            "S2 KPI section 5",
            "## 5. Job 2 — KPI par fenêtre × borough\n\n### Agrégation : fenêtre tumbling 10 min × `pickup_borough`\n\n`groupBy(window(\"tpep_pickup_datetime\", \"10 minutes\"), \"pickup_borough\")`\npuis `count(*)` + `avg(\"total_amount\")`.",
            "## 5. Job 2 — KPI par fenêtre × borough\n\n### Agrégation : fenêtre tumbling 10 min × `pickup_borough`\n\n`groupBy(window(\"tpep_pickup_datetime\", \"10 minutes\"), \"pickup_borough\")`\npuis `count(*)` + `avg(\"total_amount\")`.\n\n### Limite intrinsèque du KPI streaming (révélée par la validation cross-schema S1)\n\n`total_amount` côté stream est **monolithique** : généré par le simulateur\ndepuis `N(14.5, 9.0)` sans modélisation des composantes individuelles.\nCôté `workspace.default.bronze_yellow_taxi` (batch NYC Yellow Taxi 2025\nofficiel, 21 colonnes), `total_amount` est la **somme de 8+1 composantes\nofficielles** : `fare_amount + extra + mta_tax + tolls_amount +\nimprovement_surcharge + tip_amount + congestion_surcharge + Airport_fee\n+ cbd_congestion_fee` (la dernière ajoutée par NYC TLC post-2025).\n\n**Conséquence pour le KPI** : tant qu'on agrège `avg(total_amount)`, le\nrésultat est cohérent stream vs batch (même grandeur agrégée). Mais tout\nKPI **ventilé par composante** (tip rate, taxe rate, surcharge rate)\nest **impossible côté stream** sans étendre le simulateur. Ce serait\nle premier polish à faire pour porter ce pipeline en production réelle.",
        ),
    ],
    "S3_Qualite_Stream_LFA.dbc": [
        (
            "S3 section 4 anomalies non detectables",
            "### Ordre d'évaluation des `when`\n\nLes conditions sont évaluées **dans l'ordre** : la première qui matche\nl'emporte. On met les anomalies évidentes (MONTANT_NEGATIF,\nDISTANCE_NULLE...) en premier, l'ANOMALIE_STATISTIQUE en dernier. Un\ntrajet à -50$ ET avec un z-score élevé sera classé MONTANT_NEGATIF (plus\nprécis pour l'analyse de causes).",
            "### Ordre d'évaluation des `when`\n\nLes conditions sont évaluées **dans l'ordre** : la première qui matche\nl'emporte. On met les anomalies évidentes (MONTANT_NEGATIF,\nDISTANCE_NULLE...) en premier, l'ANOMALIE_STATISTIQUE en dernier. Un\ntrajet à -50$ ET avec un z-score élevé sera classé MONTANT_NEGATIF (plus\nprécis pour l'analyse de causes).\n\n### Anomalies non détectables faute de schéma batch officiel\n\nLa validation cross-schema S1 (20/05) montre que `bronze_stream_taxi`\nn'a que 16 colonnes vs 21 côté `workspace.default.bronze_yellow_taxi`.\nLes 9 colonnes batch absentes côté stream limitent intrinsèquement le\npérimètre de la quarantaine. Avec un simulateur étendu (qui générerait\nles 21 colonnes officielles), on pourrait ajouter :\n\n- `store_and_fwd_flag = 'Y'` (transmission différée taxi → réseau HS\n  au moment du trajet, donnée techniquement OK mais signalable).\n- `extra > 5$` ou `< 0$` (surcharge anormale : standard NYC = 0.5$\n  en pointe ou 1$ en nocturne).\n- `mta_tax ≠ 0.5$` (taxe MTA fixe par règlement NY State).\n- `improvement_surcharge ≠ 0.3$` (taxe d'amélioration fixe).\n- `congestion_surcharge` ou `cbd_congestion_fee` non conformes aux\n  règles par zone (Manhattan CBD vs reste).\n\nPour la note S3 le périmètre est les **6 types injectables par le\nsimulateur** (cf PDF). Le manque est intentionnel pédagogiquement,\nmais à mentionner si le prof pose la question des limites.",
        ),
        (
            "S3 Q2 reinjection",
            "- **Couverture statistique** : ne pas perdre les trajets longue distance\n  légitimes (course aéroport) qui sont parfois flagués `ANOMALIE_STATISTIQUE`\n  par excès de prudence du z-score.",
            "- **Couverture statistique** : ne pas perdre les trajets longue distance\n  légitimes (course aéroport) qui sont parfois flagués `ANOMALIE_STATISTIQUE`\n  par excès de prudence du z-score.\n- **Signal `store_and_fwd_flag` du batch officiel** (cf validation S1\n  du 20/05 — colonne absente côté stream simulé mais présente côté\n  `workspace.default.bronze_yellow_taxi`) : la valeur `'Y'` indique une\n  transmission différée par le taxi (panne réseau temporaire), pas une\n  donnée corrompue. Avec ce flag, un `INVERSION_TEMPS` détecté\n  uniquement sur des trames `store_and_fwd_flag='Y'` serait\n  **réinjectable en confiance** — la transmission décalée explique\n  naturellement les timestamps désynchronisés au moment de l'upload.\n  Le simulateur ne génère pas ce flag, donc on doit décider sans ce\n  signal d'autorité — c'est une limitation du périmètre pédagogique.",
        ),
    ],
    "S4_Scoring_Stream_LFA.dbc": [
        (
            "S4 Q3 bonus 4eme limite",
            "3. **Faux sens des labels** : si \"AIRPORT_LONG\" capture surtout des\n   trajets premium intra-Manhattan (à cause d'une inertie élevée), un\n   drift sur \"AIRPORT_LONG\" pourrait être attribué à tort au volume\n   aéroport alors que c'est en fait un drift sur le tip moyen\n   Manhattan.",
            "3. **Faux sens des labels** : si \"AIRPORT_LONG\" capture surtout des\n   trajets premium intra-Manhattan (à cause d'une inertie élevée), un\n   drift sur \"AIRPORT_LONG\" pourrait être attribué à tort au volume\n   aéroport alors que c'est en fait un drift sur le tip moyen\n   Manhattan.\n4. **Training/inference skew sur le schéma** (révélé par la validation\n   cross-schema S1 du 20/05) : le modèle K-Means Spark Core S5 a été\n   entraîné sur des features agrégées à partir des **21 colonnes NYC\n   officielles** (notamment `avg_fare` dérivée de `fare_amount` brut, et\n   indirectement liée à `total_amount` qui inclut les 8+1 composantes :\n   `extra`, `mta_tax`, `tolls_amount`, `improvement_surcharge`,\n   `congestion_surcharge`, `Airport_fee`, `tip_amount`,\n   `cbd_congestion_fee` — cette dernière ajoutée par NYC TLC post-2025).\n   On score le stream avec un `total_amount` **simulé monolithique** :\n   la grandeur agrégée est cohérente mais sa structure interne ne l'est\n   pas. **Effet sur le drift mesuré** : limité en pratique tant qu'on\n   raisonne sur `cluster_pca` qui est lui-même fonction de\n   `PULocationID` (mapping zone→cluster immuable), donc indépendant de\n   la composition tarifaire. Mais c'est une **limite conceptuelle\n   MLOps** classique : training set riche, inference set pauvre, à\n   monitorer en production réelle.",
        ),
    ],
    "S5_Final_LFA.dbc": [
        (
            "S5 diagramme bronze_yellow_taxi",
            " ┌─ tables de référence (statiques) ─────────────────────────────────┐\n │ ref_taxi_zones           (265 zones, Borough/Zone)                 │\n │ ref_borough_stats        (stats par borough pour z-score qualité)  │\n │ workspace.default.gold_quartiers_clustered                         │\n │   (262 zones × cluster_pca Spark Core S5)                          │\n └────────────────────────────────────────────────────────────────────┘",
            " ┌─ tables de référence (statiques) ─────────────────────────────────┐\n │ ref_taxi_zones           (265 zones, Borough/Zone)                 │\n │ ref_borough_stats        (stats par borough pour z-score qualité)  │\n │ workspace.default.gold_quartiers_clustered                         │\n │   (262 zones × cluster_pca Spark Core S5)                          │\n │ workspace.default.bronze_yellow_taxi                               │\n │   (21 cols NYC Yellow 2025, schéma de référence validation S1)    │\n └────────────────────────────────────────────────────────────────────┘",
        ),
        (
            "S5 C.2 ecarts 5eme point",
            "4. **Cohérence** : la **hiérarchie des boroughs** (Manhattan > Queens > Brooklyn en volume) doit rester cohérente entre les deux. Si la hiérarchie diffère, c'est un bug du simulateur (poids zones mal calibrés).",
            "4. **Cohérence** : la **hiérarchie des boroughs** (Manhattan > Queens > Brooklyn en volume) doit rester cohérente entre les deux. Si la hiérarchie diffère, c'est un bug du simulateur (poids zones mal calibrés).\n5. **Composition du `total_amount`** (point révélé par la validation cross-schema S1 du 20/05) : côté batch officiel NYC Yellow Taxi 2025, `total_amount` est la **somme de 8+1 composantes** (`fare_amount + extra + mta_tax + tolls_amount + improvement_surcharge + tip_amount + congestion_surcharge + Airport_fee + cbd_congestion_fee`). Le simulateur génère un `total_amount` **monolithique** ~`N(14.5, 9.0)` sans modéliser ces composantes individuellement. **Conséquence** : si on voulait décomposer le KPI (tip rate, surcharge rate, taxe rate), le batch le ferait, le stream pas. Le KPI \"montant moyen agrégé\" reste cohérent ; tout KPI ventilé serait intrinsèquement plus pauvre côté stream.",
        ),
        (
            "S5 C.3 point 7 refondu",
            "7. **Pas de bronze_taxi_trips batch côté `tp_spark_lfa`** → la validation schéma S1 (`bronze_stream_taxi` vs Spark Core S5) a dû faire fallback sur le schéma NYC Taxi canonique du PDF Simulateur. Un vrai pipeline MLOps ingère d'abord les fichiers historiques en batch puis branche le streaming sur le même schéma.",
            "7. **Schéma simulateur intentionnellement simplifié vs batch officiel NYC** → la validation cross-schema S1 (`bronze_stream_taxi` 16 cols vs `workspace.default.bronze_yellow_taxi` 21 cols, faite le 20/05) révèle **9 colonnes officielles absentes côté stream** : `RatecodeID`, `store_and_fwd_flag`, `extra`, `mta_tax`, `tolls_amount`, `improvement_surcharge`, `congestion_surcharge`, `Airport_fee` et **`cbd_congestion_fee`** (cette dernière ajoutée par NYC TLC post-2025, non documentée dans le PDF Simulateur — découverte tardive). Conséquences en production :\n   - **KPI ventilés impossibles** côté stream (cf C.2 point 5) : impossible de calculer un tip rate, surcharge rate, taxe rate sans les composantes.\n   - **Quarantaine appauvrie** côté stream (cf S3) : 4-5 types d'anomalies en plus seraient détectables avec `store_and_fwd_flag='Y'` (transmission différée), `extra > 5$` ou `< 0$`, `mta_tax ≠ 0.5$` standard, `improvement_surcharge` non-conforme.\n   - **Mismatch de types** sur 4 colonnes communes (`VendorID`, `PULocationID`, `DOLocationID`, `payment_type`) : Auto Loader infère `int` côté stream, batch est `bigint`. Équivalence cosmétique sur les volumes NYC (265 zones max tiennent en `int`), mais à acknowledger.\n   Sur un vrai pipeline MLOps, le simulateur serait étendu pour générer les 21 colonnes officielles, garantissant la **parité de schéma** stream/batch et l'utilisation symétrique des features dans Silver/Gold/Quarantaine.",
        ),
        (
            "S5 C.4 bilan ligne profondeur schema",
            "| Garanties qualité | Facile à auditer a posteriori | Quarantaine obligatoire, test de résilience nécessaire |",
            "| Garanties qualité | Facile à auditer a posteriori | Quarantaine obligatoire, test de résilience nécessaire |\n| **Profondeur schéma** | **21 cols NYC officielles** (toutes surcharges + `cbd_congestion_fee` post-2025) | **16 cols simulées** : 11 NYC + 4 pipeline/simulateur + `_rescued_data`. 9 cols batch officielles absentes (cf C.3 #7) |",
        ),
    ],
}


def patch_dbc(dbc_path: Path, patches: list[tuple[str, str, str]]) -> dict[str, str]:
    """Patche le .dbc en place. Retourne un report (label -> status)."""
    report = {}
    with zipfile.ZipFile(dbc_path, "r") as zin:
        names = zin.namelist()
        assert len(names) == 1, f"Expected 1 entry in {dbc_path.name}, got {names}"
        nb_name = names[0]
        raw = zin.read(nb_name).decode("utf-8")

    nb = json.loads(raw)
    assert nb.get("version") == "NotebookV1", f"Unexpected version: {nb.get('version')}"

    for label, old, new in patches:
        hits = 0
        for cmd in nb["commands"]:
            txt = cmd.get("command", "")
            if old in txt:
                cmd["command"] = txt.replace(old, new)
                hits += 1
        if hits == 0:
            report[label] = "MISS (snippet introuvable)"
        elif hits == 1:
            report[label] = "OK (1 cellule patchee)"
        else:
            report[label] = f"WARN ({hits} cellules patchees, attendu 1)"

    out = json.dumps(nb, ensure_ascii=False, separators=(",", ":"))
    with zipfile.ZipFile(dbc_path, "w", zipfile.ZIP_DEFLATED) as zout:
        zout.writestr(nb_name, out.encode("utf-8"))

    return report


for dbc_name, patches in PATCHES.items():
    dbc = RENDU / dbc_name
    print(f"\n=== {dbc_name} ===")
    if not dbc.exists():
        print(f"  SKIP (not found)")
        continue
    rep = patch_dbc(dbc, patches)
    for label, status in rep.items():
        print(f"  {label}: {status}")
    print(f"  -> wrote {dbc.stat().st_size:,} bytes")
