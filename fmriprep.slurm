#!/bin/bash
#SBATCH -p normal
#SBATCH -o log/%j.out
#SBATCH -e log/%j.err
#SBATCH -t 48:00:00
#SBATCH --mail-type=END   # Opts: NONE, BEGIN END, FAIL, REQUEUE, ALL
#SBATCH --mail-user=%u@stanford.edu
#------------------------------------------------------
module purge
module use /work/01329/poldrack/modules

module load intel python2
module load launcher/3.4.1
module load tacc-singularity

# extract dataset name and batch number from job name.
export DATASET_NAME=${SLURM_JOB_NAME:0:8}
export BATCH_NUM=${SLURM_JOB_NAME:15}

# set max subjects per batch. note that altering BATCH_SIZE requires also altering batch_size in fmriprep_runjobs.sh
export BATCH_SIZE=10

# calculate lines in participants.tsv to start and stop on 
export START_LINE=$(( $BATCH_SIZE * ( BATCH_NUM - 1 ) + 2 ))
export STOP_LINE=$(( $START_LINE + $BATCH_SIZE - 1))

export STAGING=/corral-repl/utexas/poldracklab/data/OpenNeuro
export LOG_DIR=${STAGING}/logs/${DATASET_NAME}

export LAUNCHER_PLUGIN_DIR=$LAUNCHER_DIR/plugins
export LAUNCHER_RMI=SLURM
export LAUNCHER_JOB_FILE=$(pwd)/${DATASET_NAME}.${SLURM_JOB_ID}
export LAUNCHER_LOG_OUTPUT=${LOG_DIR}/${SLURM_JOB_ID}-%n.out
export LAUNCHER_LOG_ERROR=${LOG_DIR}/${SLURM_JOB_ID}-%n.err

export STAGING=/corral-repl/utexas/poldracklab/data/OpenNeuro
BIDS_DIR=$STAGING/${DATASET_NAME}
BIDS_DIR=$STAGING/${DATASET_NAME}
if [ ! -d $BIDS_DIR ]; then
    echo "Folder $BIDS_DIR does not exist, please set a job id name (e.g. -J ds000030)"
    exit 1
fi

SINGULARITY_IMAGE="/work/01329/poldrack/stampede2/singularity_images/poldracklab_fmriprep-20.0.6.simg"
export SINGULARITYENV_FS_LICENSE=$HOME/.freesurfer.txt

mkdir -p $HOME/.cache/{templateflow,fmriprep}
fmriprep_ver=$( singularity run -B $HOME/.cache:/home/fmriprep/.cache $SINGULARITY_IMAGE --version | cut -d ' ' -f2 )
fmriprep_ver="${fmriprep_ver#v}"
DERIVS_DIR="${STAGING}/derivatives/${DATASET_NAME}"
OUTPUT_DIR="fmriprep-${fmriprep_ver}"

mkdir -p ${DERIVS_DIR}/freesurfer-6.0.1
mkdir -p ${DERIVS_DIR}/${OUTPUT_DIR}

# Ensure permissions for the group
setfacl -R -m g:G-802037:rwX ${DERIVS_DIR}
find ${DERIVS_DIR} -type d | xargs setfacl -R -m d:g:G-802037:rwX

# Link freesurfer from within the output folder
if [ ! -d ${DERIVS_DIR}/${OUTPUT_DIR}/freesurfer ]; then
    pushd ${DERIVS_DIR}/${OUTPUT_DIR}
    ln -s ../freesurfer-6.0.1 freesurfer
    popd
fi


# Create tasks file
rm -f $LAUNCHER_JOB_FILE
sed -n "${START_LINE},${STOP_LINE}p" $BIDS_DIR/participants.tsv |
while read line || [ -n "$line" ]; do
    subject=$( echo $line | cut -d' ' -f1 | cut -d'-' -f2 )
    if [ -d ${DERIVS_DIR}/${OUTPUT_DIR}/freesurfer/sub-$subject ]; then
        find ${DERIVS_DIR}/${OUTPUT_DIR}/freesurfer/sub-$subject -name "*IsRunning*" -delete
    fi

    # Clear work directory
    rm -rf $SCRATCH/fmriprep-work/${SLURM_JOB_NAME}-sub-$subject

    echo "singularity run -B ${BIDS_DIR}:/data -B ${DERIVS_DIR}:/derivatives -B $HOME/.cache:/home/fmriprep/.cache --cleanenv ${SINGULARITY_IMAGE} /data /derivatives/${OUTPUT_DIR} participant --participant-label $subject -vv --output-spaces MNI152NLin2009cAsym:res-2 fsnative fsaverage5 --nthreads 12 --omp-nthreads 8 -w $SCRATCH/fmriprep-work/${SLURM_JOB_NAME}-sub-$subject --skip-bids-validation --notrack --fs-license-file ${SINGULARITYENV_FS_LICENSE} --use-aroma --ignore slicetiming" >> ${LAUNCHER_JOB_FILE}
done

$LAUNCHER_DIR/paramrun
