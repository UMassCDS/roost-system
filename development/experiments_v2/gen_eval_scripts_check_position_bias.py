import os

EXP_GROUP_NAME = "08"
ROOT = EXP_GROUP_NAME
# CKPTS = range(29999, 150000, 5000)
##### move center to corners and corners to center in the test arrays to check model positional biases #####
EVAL_STRATEGY = 91  # ignore <15x15 in 1200x1200

# EXPDIR_TESTDATA = [
#     ("resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
#     ("resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 2),
#     ("resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
#     ("resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
# ]
EXPDIR_TESTDATA = [
    # ("08_1_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_1_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_1_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 2),
    # ("08_1_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 2),
    # ("08_1_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # ("08_1_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # ("08_1_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # ("08_1_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # ("08_2_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_2_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_2_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_3_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_3_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_3_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_4_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_4_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_4_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_5_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_5_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_5_seed3_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # ("08_6_seed1_resnet50-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # ("08_6_seed2_resnet50-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # ("08_6_seed3_resnet50-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # (139999, "08_2_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3), # 87
    # (139999, "08_2_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4), # 88
    (129999, "08_3_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    (129999, "08_3_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    (129999, "08_3_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # (129999, "08_3_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # (139999, "08_4_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # (139999, "08_4_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # (139999, "08_5_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # (139999, "08_5_seed1_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # (49999, "08_6_seed1_resnet50-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 1),
    # (49999, "08_6_seed1_resnet50-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    #
    # (134999, "08_2_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # (134999, "08_2_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # (139999, "08_3_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # (139999, "08_3_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # (134999, "08_4_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # (134999, "08_4_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
    # (139999, "08_5_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 3),
    # (139999, "08_5_seed2_resnet101-FPN_detptr_anc10_regsl1_imsz1200_lr0.001_it150k", 4),
]
EXCEPTION = [
    # (69999, "resnet50-FPN_noptr_anc6_imsz1200_lr0.001_it150k"),
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
for i, exp_name, test_dataset in EXPDIR_TESTDATA:
    network = exp_name.split("_")[3]
    if int(EXP_GROUP_NAME) <= 7:
        network = exp_name.split("_")[0]
    imsize = exp_name.split("_imsz")[1].split("_")[0]
    anchor_strategy = exp_name.split("_anc")[1].split("_")[0]

    # for i in CKPTS:
    if (i, exp_name) in EXCEPTION:
        continue
    ckpt_path = os.path.join(log_dir, exp_name, f"model_{i:07d}.pth")
    if not os.path.exists(ckpt_path):
        print("missing path:", ckpt_path)
    else:
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
        if exp_idx < 100:
            partition = "rtx8000-short"
        elif exp_idx < 384:
            partition = "m40-short"
        else:
            partition = "1080ti-short"
        launch_file.write(
            f"sbatch -o {slurm_dir}/{eval_name}_%J.out "
            f"-p {partition} --gres=gpu:1 --mem=100000 {script_path}\n"
        )
        exp_idx += 1
