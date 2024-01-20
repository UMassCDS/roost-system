import os
import time

NUM_CPUS = 7
# deployment station, start date (inclusive), end date (inclusive)
# specify either
# STATIONS = ["KABR", "KABX", "KAKQ"]
# TIMES = [("20220101", "20221231"),]
# STATIONS = ["KOKX"]
# TIMES = [(f"{year}0601", f"{year}1031") for year in range(2000, 2023)]
# or
STATIONS_TIMES = [
    ("KTYX", "20200805", "20200806"),
    # ("KTYX", "20200101", "20201231"),
    # ("KTYX", "20220101", "20221231"),
    # ("KLIX", "20200101", "20201231"),
    # ("KLIX", "20220101", "20221231"),
    # ("KDAX", "20200101", "20201231"),
    # ("KDAX", "20220101", "20221231"),
    # ("KTLX", "20200101", "20201231"),
    # ("KTLX", "20220101", "20221231"),
]

SPECIES = "swallow"
SUN_ACTIVITY = "sunrise" # bird activities occur around sunrise
MIN_BEFORE = 30
MIN_AFTER = 90
# directory for system outputs
MODEL_VERSION = "v3"
EXPERIMENT_NAME = f"us_sunrise_{MODEL_VERSION}_pilot0119"
DATA_ROOT = f"/mnt/nfs/scratch1/wenlongzhao/roosts_data/{EXPERIMENT_NAME}"

try:
    assert STATIONS_TIMES
    args_list = STATIONS_TIMES
except:
    args_list = [(s, t[0], t[1]) for s in STATIONS for t in TIMES]
for args in args_list:
    station = args[0]
    start = args[1]
    end = args[2]
    
    slurm_logs = f"slurm_logs/{EXPERIMENT_NAME}/{station}"
    slurm_output = os.path.join(slurm_logs, f"{station}_{start}_{end}.out")
    slurm_error = os.path.join(slurm_logs, f"{station}_{start}_{end}.err")
    os.makedirs(slurm_logs, exist_ok=True)

    os.system(f"export MKL_NUM_THREADS={NUM_CPUS}")
    os.system(f"export OPENBLAS_NUM_THREADS={NUM_CPUS}")
    os.system(f"export OMP_NUM_THREADS={NUM_CPUS}")

    cmd = f'''sbatch \
    --job-name="{station}{start}_{end}" \
    --output="{slurm_output}" \
    --error="{slurm_error}" \
    --nodes=1 \
    --ntasks=1 \
    --cpus-per-task={NUM_CPUS} \
    --mem-per-cpu=2000 \
    --partition=longq \
    --time=7-00:00:00 \
    demo.sbatch \
    --species {SPECIES} --station {station} --start {start} --end {end} \
    --sun_activity {SUN_ACTIVITY} --min_before {MIN_BEFORE} --min_after {MIN_AFTER} \
    --data_root {DATA_ROOT} --model_version {MODEL_VERSION}'''
    
    os.system(cmd)
    time.sleep(1)