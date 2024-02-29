import numpy as np
import os
import json


SPLITS = ["train", "val", "test"]
DATASET = "roosts_v0.1.0"
JSON_ROOT = "/mnt/nfs/work1/smaji/wenlongzhao/roosts/datasets/roosts_v0.1.0"
DATASET_JSON = os.path.join(JSON_ROOT, "roosts_v0.1.0.json")
SPLIT_JSON = os.path.join(JSON_ROOT, "roosts_v0.1.0_standard_splits.json")
ARRAY_DIR = "/mnt/nfs/work1/smaji/zezhoucheng/randmo_repo/roosts/libs/wsrdata/static/arrays/v0.1.0"
BAD_NPZ = open("bad_npz.txt", "w")


with open(DATASET_JSON) as f:
    dataset = json.load(f)

for split in SPLITS:
    with open(SPLIT_JSON) as f:
        scan_list = json.load(f)[split]

    for scan_id in scan_list:
        array_path = os.path.join(ARRAY_DIR, dataset["scans"][scan_id]["array_path"])

        BAD_NPZ.write(f"{array_path} - load")
        array = np.load(array_path)
        BAD_NPZ.write(f"{array_path} - array")
        _ = array["array"]
