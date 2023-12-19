#!/bin/sh

#SBATCH --job-name=mosaics
#SBATCH --mail-type=END
#SBATCH --mail-user=jbwexler@stanford.edu
#SBATCH --nodes=1
#SBATCH --ntasks=56
#SBATCH --time=03:00:00
#SBATCH --partition=small

module load launcher
export LAUNCHER_JOB_FILE="${PWD}/mosaics_launcher"

"$LAUNCHER_DIR"/paramrun
