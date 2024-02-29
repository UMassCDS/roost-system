import os

GPU_NODES_TO_EXCLUDE = "gypsum-gpu[030,035,039,096,097,098,099,122,146]"

EXP_GROUP_NAME = "09"
ROOT = EXP_GROUP_NAME
CKPTS = [99999]  # range(24999, 150000, 25000) # [99999]
EVAL_STRATEGY = 1  # ignore <15x15 in 1200x1200

EXPDIR_TESTDATA = [
    ("09_3_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4)
]
# EXPDIR_TESTDATA = [
#     (f"09_{i}_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", j) for i in [3, 4] for j in range(1, 5)
# ]
# EXPDIR_TESTDATA.extend([
#     (f"09_{i}_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", i) for i in [11] #range(8, 12)
# ])
# EXPDIR_TESTDATA.extend([
#     (f"09_{i}_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1) for i in [12, 15] #range(12, 17)
# ])
EXCEPTIONS = [
    "09_3_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
    "09_4_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
    "09_11_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
    "09_12_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
    "09_15_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
    "09_7_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
    "09_7_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
    "09_8_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k",
]


script_dir = os.path.join(ROOT, "scripts")
slurm_dir = os.path.join(ROOT, "slurm")
log_dir = os.path.join(ROOT, "logs")
os.makedirs(script_dir, exist_ok=True)
os.makedirs(slurm_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)
launch_file = open(os.path.join(script_dir, "launch_eval.sh"), "w")
launch_file.write("#!/bin/bash\n")

exp_idx = 0
for exp_name, test_dataset in EXPDIR_TESTDATA:
    network = exp_name.split("_")[3]
    if int(EXP_GROUP_NAME) <= 7:
        network = exp_name.split("_")[0]
    imsize = exp_name.split("_imsz")[1].split("_")[0]
    anchor_strategy = exp_name.split("_anc")[1].split("_")[0]

    for i in CKPTS:
        if exp_name in EXCEPTIONS:
            continue
        ckpt_path = os.path.join(log_dir, exp_name, f"model_{i:07d}.pth")
        if os.path.exists(ckpt_path):
            eval_name = f"eval{test_dataset}_{exp_name}_ckpt{i}_strt{EVAL_STRATEGY}"
            script_path = os.path.join(script_dir, eval_name + ".sbatch")
            eval_name_brief = f"eval{test_dataset}_ckpt{i}_strt{EVAL_STRATEGY}"
            output_dir = os.path.join(log_dir, exp_name, eval_name_brief)
            os.makedirs(output_dir, exist_ok=True)

            with open(script_path, "w") as f:
                f.write("#!/bin/bash\n")
                f.write("hostname\n")
                f.write(
                    "".join(
                        (
                            "python eval_roost_detector.py",
                            f" --test_dataset {test_dataset}"
                            f" --ckpt_path {ckpt_path} --eval_strategy {EVAL_STRATEGY}",
                            f" --imsize {imsize}"
                            f" --network {network} --anchor_strategy {anchor_strategy}",
                            f" --output_dir {output_dir}",
                        )
                    )
                )
            if exp_idx < 0:
                partition = "gypsum-m40-phd"
            elif exp_idx < 0:
                partition = "gypsum-titanx-phd"
            else:
                partition = "gypsum-1080ti-phd"  # TODO
            launch_file.write(
                f"sbatch -o {slurm_dir}/{eval_name}_%J.out "
                f"-p {partition} --exclude={GPU_NODES_TO_EXCLUDE} --gres=gpu:1 "
                f"--mem=100000 {script_path}\n"
            )
            exp_idx += 1
