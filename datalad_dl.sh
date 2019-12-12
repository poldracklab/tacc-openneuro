#!/bin/bash
pushd $STAGING
DATASET=$0
if [[ ! -d $DATASET ]]; then
    datalad install https://github.com/OpenNeuroDatasets/${DATASET}.git
    mkdir -p derivatives/${DATASET}
    # Ensure permissions for the group
    setfacl -R -m g:G-802037:rwX ${DATASET}
    find ${DATASET} -type d | xargs setfacl -R -m d:g:G-802037:rwX
    
    # Fetch
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
else
    # Update
    cd ${DATASET}
    datalad update --merge .
    find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude\|phasediff\|epi\)\.nii\(\.gz\)?" \
        -exec datalad get {} +
fi
popd
