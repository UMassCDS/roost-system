import os
import time

NUM_CPUS = 7
# deployment station, start date (inclusive), end date (inclusive)
# specify either
STATIONS = ["KEWX"]  # ["KDFX", "KSJT", "KGRK"]
TIMES = []
for year in range(2000, 2021):
    # for (start_date, end_date) in [("0101", "0331"), ("0401", "0630"), ("0701", "0930"), ("1001", "1231")]:
    for start_date, end_date in [("0101", "1231")]:
        TIMES.append((str(year) + start_date, str(year) + end_date))
# or
# STATIONS_TIMES = [
#     ("KDFX", "20060101", "20061231"),
# ]

SUN_ACTIVITY = "sunset"  # bat activities occur around sunset
MIN_BEFORE = 150
MIN_AFTER = 150
# directory for system outputs
MODEL_VERSION = "v3"
EXPERIMENT_NAME = f"texas_bats_{MODEL_VERSION}_long"
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

    cmd = f"""sbatch \
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
    --station {station} --start {start} --end {end} \
    --sun_activity {SUN_ACTIVITY} --min_before {MIN_BEFORE} --min_after {MIN_AFTER} \
    --data_root {DATA_ROOT} --model_version {MODEL_VERSION}"""

    os.system(cmd)
    time.sleep(1)
