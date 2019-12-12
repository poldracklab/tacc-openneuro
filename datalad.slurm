#!/bin/bash
#SBATCH -p normal
#SBATCH -o log/%j.out
#SBATCH -e log/%j.err
#SBATCH -t 08:00:00
#SBATCH --mail-type=ALL   # Opts: NONE, BEGIN, END, FAIL, REQUEUE, ALL
#SBATCH --mail-user=%u@stanford.edu  
#------------------------------------------------------
module purge
module use /work/01329/poldrack/modules

module load intel python2
module load launcher/3.4.1
module load tacc-singularity

export STAGING=/corral-repl/utexas/poldracklab/data/OpenNeuro
export LOG_DIR=${STAGING}/logs/${SLURM_JOB_NAME}
export LAUNCHER_PLUGIN_DIR=$LAUNCHER_DIR/plugins
export LAUNCHER_RMI=SLURM
export LAUNCHER_JOB_FILE=$(pwd)/${SLURM_JOB_NAME}.${SLURM_JOB_ID}
export LAUNCHER_LOG_OUTPUT=${LOG_DIR}/datalad-${SLURM_JOB_ID}-%n.out
export LAUNCHER_LOG_ERROR=${LOG_DIR}/datalad-${SLURM_JOB_ID}-%n.err

BIDS_DIR=$STAGING/${SLURM_JOB_NAME}

# Create tasks file
rm -f $LAUNCHER_JOB_FILE
cat dataset_list.txt | while read line
do
    bash datalad_dl.sh $line
done

$LAUNCHER_DIR/paramrun
