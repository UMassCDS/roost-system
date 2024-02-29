import os

GPU_NODES_TO_EXCLUDE = "gypsum-gpu[030,035,039,096,097,098,099,122,146]"

EXP_GROUP_NAME = "09"
ROOT = EXP_GROUP_NAME
MAX_ITER = 150000
CKPT_PERIOD = 5000

TRAINDATA_NET_IMSZ_ANCHOR_REGLOSS = [
    (i, "resnet101-FPN", 1200, 10, "smooth_l1") for i in range(1, 17)
]
SEED = [1, 2, 3]
PRETRAIN_LR = [
    ("det", 0.001),
]
FLIP_AND_ROTATE = [
    (False, False),
]
FILTER_EMPTY = [
    False,
]


script_dir = os.path.join(ROOT, "scripts")
slurm_dir = os.path.join(ROOT, "slurm")
log_dir = os.path.join(ROOT, "logs")
os.makedirs(script_dir, exist_ok=True)
os.makedirs(slurm_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)
launch_file = open(os.path.join(script_dir, "launch_train.sh"), "w")
launch_file.write("#!/bin/bash\n")

exp_idx = 0
for seed in SEED:
    for (
        train_dataset,
        network,
        imsize,
        anchor_strategy,
        reg_loss,
    ) in TRAINDATA_NET_IMSZ_ANCHOR_REGLOSS:
        for pretrain, lr in PRETRAIN_LR:
            for flip, rotate in FLIP_AND_ROTATE:
                for filter_empty in FILTER_EMPTY:
                    exp_name = (
                        f"{EXP_GROUP_NAME}_{train_dataset}_seed{seed}"
                        f"_{network}_{pretrain}ptr_anc{anchor_strategy}"
                        f"_reg{'giou' if reg_loss=='giou' else 'sl1'}"
                        f"_imsz{imsize}"
                        f"{'_flip' if flip else ''}{'_rot' if rotate else ''}"
                        f"{'_flt' if filter_empty else ''}"
                        f"_lr{lr:.3f}_it{MAX_ITER//1000}k"
                    )
                    script_path = os.path.join(script_dir, exp_name + ".sbatch")
                    output_dir = os.path.join(log_dir, exp_name)
                    os.makedirs(output_dir, exist_ok=True)

                    with open(script_path, "w") as f:
                        f.write("#!/bin/bash\n")
                        f.write("hostname\n")
                        f.write(
                            "".join(
                                (
                                    "python train_roost_detector.py",
                                    f" --train_dataset {train_dataset}"
                                    f" --imsize {imsize}",
                                    " --flip" if flip else "",
                                    " --rotate" if rotate else "",
                                    " --filter_empty" if filter_empty else "",
                                    f" --seed {seed}",
                                    f" --network {network} --pretrain {pretrain}"
                                    f" --anchor_strategy {anchor_strategy} --reg_loss {reg_loss}",
                                    f" --lr {lr} --max_iter {MAX_ITER}",
                                    f" --checkpoint_period {CKPT_PERIOD} --output_dir {output_dir}",
                                )
                            )
                        )
                    partition = (
                        "gypsum-1080ti-phd" if exp_idx < 40 else "gypsum-titanx-phd"
                    )  # TODO
                    launch_file.write(
                        f"sbatch -o {slurm_dir}/{exp_name}_%J.out "
                        f"-p {partition} --exclude={GPU_NODES_TO_EXCLUDE} --gres=gpu:1 "
                        f"--mem=100000 {script_path}\n"
                    )
                    exp_idx += 1
