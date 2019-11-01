#!/bin/bash

module purge
module use /work/01329/poldrack/modules
module load anaconda

pushd $STAGING

datalad install https://github.com/OpenNeuroDatasets/${DATASET}.git
mkdir -p derivatives/${DATASET}
cd ${DATASET}
rm -r derivatives/
ln -s ../derivatives/${DATASET} derivatives
find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude\|phasediff\|epi\)\.nii\(\.gz\)?" \
    -exec datalad get {} +

## Just in case there is no participants.tsv file, create one.
if [ ! -f participants.tsv ]; then
    echo "participant_id" > participants.tsv
    find -maxdepth 1 -type d -name "sub-*" | cut -c3- | sort >> participants.tsv
fi
