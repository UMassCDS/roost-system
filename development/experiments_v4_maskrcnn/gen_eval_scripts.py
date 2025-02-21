import numpy as np
import os
import pdb
import sys

GPU_NODES_TO_EXCLUDE = None # "gypsum-gpu[030,035,039,096,097,098,099,122,146]"

EXP_GROUP_NAME = "10"
ROOT = EXP_GROUP_NAME
CKPTS = range(24999, 150000, 25000) # [99999]
EVAL_STRATEGY = 1 # ignore <15x15 in 1200x1200

EXPDIR_TESTDATA = [
    (f"10_3_seed1_mrcnn-r101-FPN_detptr_anc10_linadap_chnl{i}_imsz1000_lr0.001_it150k", 3) for i in range(1, 10)
]
EXCEPTIONS = []


script_dir = os.path.join(ROOT, 'scripts')
slurm_dir = os.path.join(ROOT, 'slurm')
log_dir = os.path.join(ROOT, 'logs')
os.makedirs(script_dir, exist_ok=True)
os.makedirs(slurm_dir, exist_ok=True)
os.makedirs(log_dir, exist_ok=True)
launch_file = open(os.path.join(script_dir, 'launch_eval.sh'), 'w')
launch_file.write('#!/bin/bash\n')

exp_idx = 0
for (exp_name, test_dataset) in EXPDIR_TESTDATA:
    network = exp_name.split("_")[3]
    if int(EXP_GROUP_NAME) <= 7: network = exp_name.split("_")[0]
    imsize = exp_name.split("_imsz")[1].split("_")[0]
    anchor_strategy = exp_name.split("_anc")[1].split("_")[0]
    adaptor_brief = exp_name.split("adap_")[0].split("_")[-1]
    if adaptor_brief == "lin":
        adaptor = "linear"
    elif adaptor_brief == "mul":
        adaptor = "multi-layer"
    channel_strategy = exp_name.split("_chnl")[1].split("_")[0]

    for i in CKPTS:
        if exp_name in EXCEPTIONS: continue
        ckpt_path = os.path.join(log_dir, exp_name, f"model_{i:07d}.pth")
        if os.path.exists(ckpt_path):
            eval_name = f"eval{test_dataset}_{exp_name}_ckpt{i}_strt{EVAL_STRATEGY}"
            script_path = os.path.join(script_dir, eval_name+".sbatch")
            eval_name_brief = f"eval{test_dataset}_ckpt{i}_strt{EVAL_STRATEGY}"
            output_dir = os.path.join(log_dir, exp_name, eval_name_brief)
            os.makedirs(output_dir, exist_ok=True)

            with open(script_path, 'w') as f:
                f.write('#!/bin/bash\n')
                f.write('hostname\n')
                f.write(
                    ''.join((
                            f'python eval_roost_detector.py',
                            f' --test_dataset {test_dataset}'
                            f' --ckpt_path {ckpt_path} --eval_strategy {EVAL_STRATEGY}',
                            f' --imsize {imsize}'
                            f' --network {network} --anchor_strategy {anchor_strategy}',
                            f' --adaptor {adaptor} --channel_strategy {channel_strategy}',
                            f' --output_dir {output_dir}',
                    ))
                )
            if exp_idx < 100:
                partition = "gypsum-titanx"
            elif exp_idx < 40:
                partition = "gypsum-1080ti"
            else:
                partition = "gypsum-titanx" # TODO
            launch_file.write(
                ''.join((
                    f'sbatch -o {slurm_dir}/{eval_name}_%J.out',
                    f' -p {partition} --gres=gpu:1',
                    f' --exclude={GPU_NODES_TO_EXCLUDE}' if GPU_NODES_TO_EXCLUDE else '',
                    f' --mem=100000 {script_path}\n',
                ))
            )
            exp_idx += 1

