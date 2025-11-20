#!/bin/bash
#SBATCH --job-name=prestogp_applied_targets
#SBATCH --output=slurm_messages/job%j.out
#SBATCH --error=slurm_messages/job_%j.err
#SBATCH --partition=highmem
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=500G
#SBATCH --mail-user=kate.pogue@nih.gov
#SBATCH --mail-type=BEGIN,END,FAIL

# --- minimal prep ---
set -eu

# Project + container (resolve to the submit directory)
PROJECT_ROOT="/ddn/gs1/group/set/PrestoGP_PM"
CONTAINER_SIF="${PROJECT_ROOT}/container_prestogp_applied.sif"

# Threading hygiene
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export NUMEXPR_NUM_THREADS=1
export R_BAM_THREADS="${SLURM_CPUS_PER_TASK}"

# Run the targets pipeline inside the container
apptainer exec \
  --bind "${PROJECT_ROOT}:${PROJECT_ROOT}" \
  --pwd  "${PROJECT_ROOT}" \
  "${CONTAINER_SIF}" \
  R -q -e "targets::tar_make()"
