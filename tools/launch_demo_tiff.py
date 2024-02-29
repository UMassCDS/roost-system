import os
import time

NUM_CPUS = 7
# directory for system outputs
EXPERIMENT_NAME = "bartk"
DATA_ROOT = f"/mnt/nfs/scratch1/wenlongzhao/roosts_data/{EXPERIMENT_NAME}"

slurm_logs = f"slurm_logs/{EXPERIMENT_NAME}"
slurm_output = os.path.join(slurm_logs, "log.out")
slurm_error = os.path.join(slurm_logs, "log.err")
os.makedirs(slurm_logs, exist_ok=True)

os.system(f"export MKL_NUM_THREADS={NUM_CPUS}")
os.system(f"export OPENBLAS_NUM_THREADS={NUM_CPUS}")
os.system(f"export OMP_NUM_THREADS={NUM_CPUS}")

cmd = f"""sbatch \
--output="{slurm_output}" \
--error="{slurm_error}" \
--nodes=1 \
--ntasks=1 \
--cpus-per-task={NUM_CPUS} \
--mem-per-cpu=2000 \
--partition=longq \
--time=2-00:00:00 \
demo_tiff.sbatch --data_root {DATA_ROOT}"""

os.system(cmd)
time.sleep(1)
