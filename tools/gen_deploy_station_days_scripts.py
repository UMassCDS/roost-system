import os


NUM_CPUS = 7
# station, start date (inclusive), end date (inclusive)
STATIONS_TIMES = {
    "KDAX": [
        ("20100401", "20100401"),
        ("20100501", "20100501"),
    ],
    "KMTX": [
        ("20100401", "20100401"),
        ("20100501", "20100501"),
    ],
}

SUN_ACTIVITY = "sunrise"
MIN_BEFORE = 30
MIN_AFTER = 90
# directory for system outputs
MODEL_VERSION = "v2"
EXPERIMENT_NAME = f"try_202207_{MODEL_VERSION}"
DATA_ROOT = f"/mnt/nfs/scratch1/wenlongzhao/roosts_data/{EXPERIMENT_NAME}"

script_dir = "scripts"
os.makedirs(script_dir, exist_ok=True)
launch_file = open(
    os.path.join(script_dir, "launch_deploy_station_days_scripts.sh"), "w"
)
launch_file.write("#!/bin/bash\n")
launch_file.write(f"export MKL_NUM_THREADS={NUM_CPUS}\n")
launch_file.write(f"export OPENBLAS_NUM_THREADS={NUM_CPUS}\n")
launch_file.write(f"export OMP_NUM_THREADS={NUM_CPUS}\n")

for station in STATIONS_TIMES:
    slurm_logs = f"slurm_logs/{EXPERIMENT_NAME}/{station}"
    slurm_output = os.path.join(slurm_logs, f"{station}.out")
    slurm_error = os.path.join(slurm_logs, f"{station}.err")
    os.makedirs(slurm_logs, exist_ok=True)

    script_path = os.path.join(script_dir, station + ".sbatch")
    with open(script_path, "w") as f:
        f.write("#!/bin/bash\n")
        f.write("hostname\n")
        for start, end in STATIONS_TIMES[station]:
            f.write(
                "".join(
                    (
                        "python demo.py",
                        f" --station {station} --start {start} --end {end}",
                        f" --sun_activity {SUN_ACTIVITY} --min_before {MIN_BEFORE} --min_after {MIN_AFTER}",
                        f" --data_root {DATA_ROOT} --model_version {MODEL_VERSION};\n",
                    )
                )
            )

    launch_file.write(
        "".join(
            (
                f"sbatch --job-name={station} --output={slurm_output} --error={slurm_error}",
                f" --nodes=1 --ntasks=1 --cpus-per-task={NUM_CPUS} --mem-per-cpu=2000",
                f" --partition=longq --time=2-00:00:00 {script_path}\n",
            )
        )
    )
