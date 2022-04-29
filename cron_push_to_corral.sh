#!/bin/bash

source ~/.bashrc
conda activate main
set -eu

showq=$(showq -u)
for ds_path in $STAGING/derivatives/*/ds*;do
	ds_basename=$(basename "$ds_path")
	ds="${ds_basename:0:8}"
	if echo "$showq" | grep -iq "${ds}"; then
		cd "$ds_path"
		datalad save
		datalad push --to origin
	fi
done
