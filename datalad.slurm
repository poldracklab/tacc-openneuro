#!/bin/bash
#SBATCH -J dlopenneuro
#SBATCH -p normal
#SBATCH -o log/%j.out
#SBATCH -e log/%j.err
#SBATCH -t 08:00:00
#SBATCH --mail-type=ALL   # Opts: NONE, BEGIN, END, FAIL, REQUEUE, ALL
#SBATCH --mail-user=%u@stanford.edu  
#------------------------------------------------------
module purge
module use /work/01329/poldrack/modules
module load anaconda

export STAGING=/corral-repl/utexas/poldracklab/data/OpenNeuro
export LOG_DIR=${STAGING}/logs/${SLURM_JOB_NAME}
export LAUNCHER_PLUGIN_DIR=$LAUNCHER_DIR/plugins
export LAUNCHER_RMI=SLURM
export LAUNCHER_JOB_FILE=$(pwd)/datalad-${SLURM_JOB_NAME}.${SLURM_JOB_ID}
export LAUNCHER_LOG_OUTPUT=${LOG_DIR}/datalad-${SLURM_JOB_ID}-%n.out
export LAUNCHER_LOG_ERROR=${LOG_DIR}/datalad-${SLURM_JOB_ID}-%n.err

# Create tasks file
rm -f $LAUNCHER_JOB_FILE
cat dataset_list.txt | while read line
do
    echo "bash datalad_dl.sh $line" >> $LAUNCHER_JOB_FILE
done

$LAUNCHER_DIR/paramrun
