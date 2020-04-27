#!/bin/bash

if [[ $1 ]]; then
    DATASET=$1
fi
LOG_DIR=$STAGING/logs/$DATASET
mkdir -p $LOG_DIR $PWD/log

# set max subjects per batch. note that altering BATCH_SIZE requires also altering batch_size in fmriprep.slurm
batch_size=10

# get total ntasks and nbatches
ntasks_total=$( tail -n +2 ${STAGING}/${DATASET}/participants.tsv | grep -c . )
rem=$(($ntasks_total % $batch_size ))
nbatches=$(($ntasks_total / batch_size + 1))
echo $ntasks_total
# launch batches
for batch in $(seq 1 $nbatches); do
    # if last batch, ntasks = remaining number tasks. otherwise ntasks = batch_size
    if [[ $batch == $nbatches ]]; then
        ntasks=$rem
    else
        ntasks=$batch_size
    fi
    sbatch -J ${DATASET}_batch-${batch} -n $ntasks -N $ntasks fmriprep.slurm
done
