#!/bin/sh
#SBATCH --mail-type=END
#SBATCH --mail-user=jbwexler@stanford.edu
#SBATCH -o $OPENNEURO/mosaics/logs/%x.out
#SBATCH -e $OPENNEURO/mosaics/logs/%x.err
#SBATCH --mail-type=END
#SBATCH --nodes=1
#SBATCH --ntasks=56
#SBATCH --time=00:05:00
#SBATCH --partition=small

module load launcher
export LAUNCHER_JOB_FILE="${PWD}/mosaics_launcher"

"$LAUNCHER_DIR"/paramrun
