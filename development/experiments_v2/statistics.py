# bbox size statistics for roost datasets
# to determine the anchor sizes for models

import json
import numpy as np

DATASET_JSON = (
    "/mnt/nfs/work1/smaji/wenlongzhao/roosts/datasets/roosts_v0.1.0/roosts_v0.1.0.json"
)

with open(DATASET_JSON) as f:
    dataset = json.load(f)
attributes = dataset["info"]["array_fields"]
elevations = dataset["info"]["array_elevations"]

sizes = []
for anno in dataset["annotations"]:
    sizes.append((anno["bbox"][2] + anno["bbox"][3]) / 2.0)

np.set_printoptions(precision=2)
hist, bins = np.histogram(sizes, 30)
print("hist", hist)
print("normalized hist", hist / sum(hist))
print(
    "min",
    min(sizes),
    "max",
    max(sizes),
    "mean",
    np.mean(sizes),
    "median",
    np.median(sizes),
)
print("bins", bins)
