# COCO evaluation using customized IoU thresholds
# sbatch -o slurm.out -p gypsum-titanx-phd --gres=gpu:0 --mem=10GB custom_metrics.sbatch

import json
import os
import numpy as np
from pycocotools.coco import COCO
from roosts.evaluation.roosteval import COCOeval

EXP_GROUP_NAME = "09"
EXP_GROUP_DIR = (
    "/work/wenlongzhao_umass_edu/roosts/roost-system/development/experiments/09/logs/"
)
DATASETS = {
    1: "v0.1.0_standard",
    2: "v0.2.0_no_dualpol",
    3: "v0.2.0_dualpol",
    4: "v0.2.0_standard",
    5: "v0.2.0_add_no_dualpol",
    6: "v0.2.0_add_dualpol",
    7: "v0.2.0_add_standard",
    8: "v0.2.0_station1",
    9: "v0.2.0_station2",
    10: "v0.2.0_station3",
    11: "v0.2.0_station4",
    12: "v0.2.1_add_no_dualpol",
    13: "v0.2.2_add_no_dualpol",
    14: "v0.2.3_add_no_dualpol",
    15: "v0.2.4_add_no_dualpol",
    16: "v0.2.5_add_no_dualpol",
}

IOUS = [0.4, 0.3, 0.2]  # from large to small
CKPTS = range(24999, 150000, 25000)
EVAL_SETTINGS = [
    (f"09_{i}_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", j)
    for i in [1, 2]
    for j in range(1, 5)
]
EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", j)
        for i in [3, 4]
        for j in range(1, 5)
    ]
)
EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", j)
        for i in range(5, 8)
        for j in range(1, 5)
    ]
)

EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", i)
        for i in range(8, 11)
    ]
)
EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", i)
        for i in [11]
    ]
)

EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1)
        for i in [12]  # range(12, 17)
    ]
)
EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1)
        for i in range(13, 15)
    ]
)
EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1)
        for i in [15]  # range(12, 17)
    ]
)
EVAL_SETTINGS.extend(
    [
        (f"09_{i}_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1)
        for i in [16]  # range(12, 17)
    ]
)

outputs = []
exp_names = []
for exp_name, test_dataset in EVAL_SETTINGS:
    AP = {iou: {} for iou in IOUS}  # inner {} for checkpoints
    exp_names.append(exp_name + "\n")

    for ckpt in CKPTS:
        # test dataset
        coco_gt = COCO(
            os.path.join(
                EXP_GROUP_DIR,
                exp_name,
                f"eval{test_dataset}_ckpt{ckpt}_strt1",
                f"{DATASETS[test_dataset]}_test_coco_format.json",
            )
        )

        # predictions
        with open(
            os.path.join(
                EXP_GROUP_DIR,
                exp_name,
                f"eval{test_dataset}_ckpt{ckpt}_strt1",
                "coco_instances_results.json",
            ),
            "r",
        ) as f:
            prediction_json = json.load(f)
        coco_dt = coco_gt.loadRes(prediction_json)

        # evaluate
        coco_eval = COCOeval(coco_gt, coco_dt, "bbox")
        coco_eval.params.iouThrs = np.linspace(
            np.min(IOUS), np.max(IOUS), len(IOUS), endpoint=True
        )
        coco_eval.evaluate()
        coco_eval.accumulate()
        # coco_eval.eval['precision'].shape = (n_IoUs, 101, 1, 4, 3)
        # IoU thres
        # recall thres 0:0.01:1
        # 1 class
        # gt filter [[0 ** 2, 1e5 ** 2], [0 ** 2, 32 ** 2], [32 ** 2, 96 ** 2], [96 ** 2, 1e5 ** 2]]
        # dt filter top-scoring [1, 10, 100]

        for i, iou in enumerate(IOUS):
            AP[iou][ckpt] = np.mean(
                coco_eval.eval["precision"][len(IOUS) - 1 - i, :, 0, 0, -1]
            )

    output = ""
    for iou in IOUS:
        for ckpt in CKPTS:
            output += f"{AP[iou][ckpt]*100:.3f}"
            if iou == IOUS[-1] and ckpt == CKPTS[-1]:
                output += "\n"
                outputs.append(output)
            else:
                output += "\t"

with open("collected_results.txt", "w") as f:
    f.writelines(outputs)
with open("collected_exp_names.txt", "w") as f:
    f.writelines(exp_names)
