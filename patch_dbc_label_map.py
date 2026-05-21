"""Patch ponctuel du point #8 ("Ce qui ne fonctionne pas") du rapport S5
pour aligner sur l'etat reel Databricks (LABEL_MAP inverse vs centroides).

Necessaire parce que le contenu importe initialement dans Databricks (et
donc dans le .dbc) pretendait que le mapping etait corrige, alors qu'en
realite le code S4 sur Databricks contient toujours :
  LABEL_MAP = {0:"URBAIN_CENTRE", 1:"HUB_VOLUME", 2:"BANLIEUE_GREEN", 3:"LONG_COURRIER"}
qui ne correspond pas aux centroides Spark Core S5.

Decision Lorenzo 21/05 (matin soutenance) : on assume l'etat actuel
plutot que de re-runner les cellules pipeline + drift + analyse.
"""
from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

RENDU = Path(__file__).parent
DBC = RENDU / "S5_Final_LFA.dbc"

OLD = "8. **`LABEL_MAP` S4 inversé** → après inspection des centroïdes Spark Core S5, je vois que mon mapping `0=URBAIN_CENTRE, 2=BANLIEUE_GREEN` est cosmétiquement faux (cluster 2 a 432k trajets/zone sur 2.5 mi = c'est URBAIN_CENTRE; cluster 0 a 184 zones banlieue à 5 % Green = c'est BANLIEUE_GREEN). Le pipeline détecte quand même le drift correctement (renommage cosmétique), mais en production je trierais les labels via les centroïdes plutôt qu'à la main."

NEW = "8. **`LABEL_MAP` S4 intentionnellement laissé inversé vs centroïdes** → le mapping actuel codé en dur dans S4 ne correspond pas aux statistiques des centroïdes Spark Core S5. État réel des clusters (cf `stats_par_cluster` §4 du notebook S4) : cluster 0 = 184 zones banlieue (avg_dist 7 mi, green_ratio 5 %), cluster 1 = 26 zones long courrier (avg_dist 19 mi), cluster 2 = 43 zones Midtown (432 k trajets/zone), cluster 3 = 9 zones premium (avg_fare $84, tip $8.86). Mapping appliqué côté code : `{0: \"URBAIN_CENTRE\", 1: \"HUB_VOLUME\", 2: \"BANLIEUE_GREEN\", 3: \"LONG_COURRIER\"}` — donc le cluster 0 (banlieue) hérite du label `URBAIN_CENTRE`, le cluster 2 (Midtown) du label `BANLIEUE_GREEN`, etc. C'est un **renommage cosmétique** assumé : le pipeline produit les bonnes prédictions (mapping zone → `cluster_pca` immuable post-entraînement), seuls les libellés métier sont décalés. La leçon retenue : en MLOps production, on **dérive automatiquement les labels** à partir des centroïdes (règles sur `avg_distance`, `trips_count`, `green_taxi_ratio`) plutôt que de les figer à la main — c'est ce que je ferais sur un vrai projet."

with zipfile.ZipFile(DBC, "r") as zin:
    names = zin.namelist()
    nb_name = names[0]
    raw = zin.read(nb_name).decode("utf-8")

nb = json.loads(raw)
hits = 0
for cmd in nb["commands"]:
    txt = cmd.get("command", "")
    if OLD in txt:
        cmd["command"] = txt.replace(OLD, NEW)
        hits += 1

print(f"S5 C.3 point #8 LABEL_MAP inversé : {hits} cellule(s) patchée(s)")
assert hits == 1, f"Expected exactly 1 hit, got {hits}"

out = json.dumps(nb, ensure_ascii=False, separators=(",", ":"))
with zipfile.ZipFile(DBC, "w", zipfile.ZIP_DEFLATED) as zout:
    zout.writestr(nb_name, out.encode("utf-8"))

print(f"-> wrote {DBC.stat().st_size:,} bytes")
