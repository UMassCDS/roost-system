EXP_GROUP_NAME = "09"
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
    outputs.append(
        "\t".join(
            [
                open(
                    f"{EXP_GROUP_NAME}/logs/{exp_name}/eval{test_dataset}_ckpt{i}_strt1/eval.log",
                    "r",
                )
                .readlines()[-1]
                .split("|")[2][1:-1]
                for i in CKPTS
            ]
        )
        + "\n"
    )
    exp_names.append(exp_name + "\n")

with open(f"{EXP_GROUP_NAME}/logs/collected_results.txt", "w") as f:
    f.writelines(outputs)
with open(f"{EXP_GROUP_NAME}/logs/collected_exp_names.txt", "w") as f:
    f.writelines(exp_names)
