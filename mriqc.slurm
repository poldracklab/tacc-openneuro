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
export LAUNCHER_LOG_OUTPUT=${LOG_DIR}/${SLURM_JOB_ID}-%n.out
export LAUNCHER_LOG_ERROR=${LOG_DIR}/${SLURM_JOB_ID}-%n.err

BIDS_DIR=$STAGING/${SLURM_JOB_NAME}

if [ ! -d $BIDS_DIR ]; then
    echo "Folder $BIDS_DIR does not exist, please set a job id name (e.g. -J ds000030)"
    exit 1
fi

SINGULARITY_IMAGE="/work/01329/poldrack/stampede2/singularity_images/poldracklab_mriqc_0.15.0-2019-04-05-a4441b87badc.simg"
export SINGULARITYENV_FS_LICENSE=$HOME/.freesurfer.txt

mkdir -p $HOME/.cache/templateflow
fmriprep_ver=$( singularity run $SINGULARITY_IMAGE --version | cut -d ' ' -f2 )
fmriprep_ver="${fmriprep_ver#v}"
DERIVS_DIR="${STAGING}/derivatives/${SLURM_JOB_NAME}"
OUTPUT_DIR="mriqc-${fmriprep_ver}"

# Ensure permissions for the group
setfacl -R -m g:G-802037:rwX ${DERIVS_DIR}
find ${DERIVS_DIR} -type d | xargs setfacl -R -m d:g:G-802037:rwX

# Create tasks file
rm -f $LAUNCHER_JOB_FILE
tail --lines=+2 $BIDS_DIR/participants.tsv | while read line
do
    subject=$( echo $line | cut -d' ' -f1 | cut -d'-' -f2 )

    echo "singularity run -B ${BIDS_DIR}:/data -B ${DERIVS_DIR}:/derivatives -B $HOME/.cache/templateflow:/opt/templateflow --cleanenv ${SINGULARITY_IMAGE} /data /derivatives/${OUTPUT_DIR} participant --participant-label $subject -vv --nprocs 12 --ants-nthreads 8 -w $SCRATCH/mriqc-work/${SLURM_JOB_NAME}-sub-$subject --verbose-reports --dsname $DATASET --ica" >> ${LAUNCHER_JOB_FILE}
done

$LAUNCHER_DIR/paramrun
