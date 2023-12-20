#!/bin/sh
#SBATCH --mail-type=END
#SBATCH --mail-user=jbwexler@stanford.edu
#SBATCH --output=/corral-repl/utexas/poldracklab/data/OpenNeuro/mosaics/logs/slurm/%x.out
#SBATCH --error=/corral-repl/utexas/poldracklab/data/OpenNeuro/mosaics/logs/slurm/%x.err
#SBATCH --mail-type=END
#SBATCH --nodes=1
#SBATCH --ntasks=56
#SBATCH --time=03:00:00
#SBATCH --partition=small

module load launcher
export LAUNCHER_JOB_FILE="/corral-repl/utexas/poldracklab/data/OpenNeuro/mosaics/mosaics_launcher"

"$LAUNCHER_DIR"/paramrun
