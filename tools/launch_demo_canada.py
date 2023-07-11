import os
import time

NUM_CPUS = 7
# deployment station, start date (inclusive), end date (inclusive)
# specify either
STATIONS = ["CSET"]
TIMES = [("20220603", "20220604"),]
# or
# STATIONS_TIMES = [
#     ("KLTX", "20100701", "20100701"),
# ]

SUN_ACTIVITY = "sunrise" # bird activities occur around sunrise
MIN_BEFORE = 30
MIN_AFTER = 60
# directory for system outputs
MODEL_VERSION = "v3"
EXPERIMENT_NAME = f"canadian_test_{MODEL_VERSION}"
DATA_ROOT = f"~/cdsfork/roost-data/{EXPERIMENT_NAME}"

try:
    assert STATIONS_TIMES
    args_list = STATIONS_TIMES
except:
    args_list = [(s, t[0], t[1]) for s in STATIONS for t in TIMES]
for args in args_list:
    station = args[0]
    start = args[1]
    end = args[2]
    is_canadian = True
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
    --partition=cpu-long \
    --time=4-00:00:00 \
    demo_canada.sbatch \
    --station {station} --start {start} --end {end} --is_canadian={is_canadian}\
    --sun_activity {SUN_ACTIVITY} --min_before {MIN_BEFORE} --min_after {MIN_AFTER} \
    --data_root {DATA_ROOT} --model_version {MODEL_VERSION}'''
    
    os.system(cmd)
    time.sleep(1)
