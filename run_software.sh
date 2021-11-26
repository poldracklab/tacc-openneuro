#!/bin/bash

# Treat unset variables as an error; Exit immediately if a command exits with a non-zero status
#set -u -e

# Clone/update raw datasets and download necessary data for freesurfer/fmriprep/mriqc
download_raw_ds () {
	echo "$dataset_list" | while read raw_ds || [ -n "$line" ]; do
		raw_path="$STAGING/raw/$raw_ds"

		if [[ ! -d "$raw_path" ]]; then
			datalad clone https://github.com/OpenNeuroDatasets/${raw_ds}.git "$raw_path"

			# Ensure permissions for the group
			setfacl -R -m g:G-802037:rwX "$raw_path"
			find "$raw_path" -type d | xargs setfacl -R -m d:g:G-802037:rwX
	  
			# Get files from corral if available
			corral_ds="$OPENNEURO/raw/$raw_ds"
			if [[ -d "$corral_ds" ]]; then
				datalad update --merge -d "$corral_ds"
				datalad siblings configure -d "$corral_ds" -s scratch --url "$raw_path"
				datalad push -d "$corral_ds" --to scratch --data anything
			fi

			cd "$raw_path" 
			find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
				-exec datalad get {} +
		else
			# Update
			cd "$raw_path"
			datalad update --merge
			find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
				-exec datalad get {} +
		fi
	done
}

# Create derivatives dataset if necessary
create_derivatives_ds () {
	echo "$dataset_list" | while read raw_ds || [ -n "$line" ]; do
		derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
		raw_path="$STAGING/raw/$raw_ds"

		# To-do: fix reckless clones so they link to original URL; add line to download licenses; fix reckless clone issue when original dataset doesn't download properly 
		if [[ ! -d "$derivatives_path" ]]; then
			datalad create -c yoda "$derivatives_path"
			cd "$derivatives_path"
			datalad clone -d . "$STAGING/containers" code/containers --reckless ephemeral
			git clone https://github.com/poldracklab/tacc-openneuro.git code/tacc-openneuro
			mkdir sourcedata
			datalad clone -d . "$raw_path" sourcedata/raw --reckless ephemeral
	  
			cp code/tacc-openneuro/gitattributes_openneuro.txt .gitattributes
			cp code/tacc-openneuro/gitattributes_datalad_openneuro.txt .datalad/.gitattributes

			if [[ "$software" == "fmriprep" ]]; then
				# Create freesurfer dataset
				# todo: look for existing freesurfer derivatives. don't need to alter fmriprep command because it will already look in fs dir
				fs_path="$derivatives_path/sourcedata/freesurfer"
				datalad create -c yoda "$fs_path"
				cd "$fs_path"
				datalad clone -d . ///repronim/containers code/containers
				git clone https://github.com/poldracklab/tacc-openneuro.git code/tacc-openneuro
				mkdir sourcedata
				datalad clone -d . https://github.com/OpenNeuroDatasets/"${raw_ds}".git sourcedata/raw
		
				cp code/tacc-openneuro/gitattributes_openneuro.txt .gitattributes
				cp code/tacc-openneuro/gitattributes_datalad_openneuro.txt .datalad/.gitattributes
			fi
	  
			# Ensure permissions for the group
			setfacl -R -m g:G-802037:rwX "$derivatives_path"
			find "$derivatives_path" -type d | xargs setfacl -R -m d:g:G-802037:rwX
		else
			# Update
			datalad update --merge -d "$derivatives_path/sourcedata/raw"
			datalad update --merge -d "$derivatives_path/code/containers"
			datalad update --merge -d "$derivatives_path/code/tacc-openneuro"	  
		fi
	done
}

# Run fmriprep or mriqc
run_software () {
	echo "$dataset_list" | while read raw_ds || [ -n "$line" ]; do  
		derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}" 
		raw_path="$STAGING/raw/$raw_ds"
		cd "$derivatives_path"
	
		if [[ "$software" == "fmriprep" ]]; then
			walltime="48:00:00"
			killjob_factors=".75,.15"
			if [ -z "$subs_per_node" ]; then
				subs_per_node=4
			fi
			mem_mb="$(( 150000 / $subs_per_node ))"
			command=("--output-spaces" "MNI152NLin2009cAsym:res-2" "anat" "func" "fsaverage5" "--nthreads" "14" \
				"--omp-nthreads" "7" "--skip-bids-validation" "--notrack" "--fs-license-file" "$fs_license" \
					"--use-aroma" "--ignore" "slicetiming" "--output-layout" "bids" "--cifti-output" "--resource-monitor" \
						"--skull-strip-t1w" "$skull_strip" "$syn_sdc" "--mem_mb" "$mem_mb" "--bids-database-dir" "/tmp")
		elif [[ "$software" == "mriqc" ]]; then
			walltime="8:00:00"
			killjob_factors=".85,.15"
			if [ -z "$subs_per_node" ]; then
				subs_per_node=5
			fi
			mem_mb="$(( 150 / $subs_per_node ))"
			command=("--nprocs" "11" "--ants-nthreads" "8" "--verbose-reports" "--dsname" "$raw_ds" "--ica" "--mem_gb" "$mem_mb")
		fi
	
		if [ -z "$all_subs_arg" ]; then
			all_subs=$(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' | sort)
		else
			all_subs=$(echo "$all_subs_arg" | sed 's/,/\n/g')
		fi
		
		# Remove old work dirs
		if [[ "$skip_workdir_delete" == "False" ]]; then
			for sub in $all_subs; do
				rm -rf "$work_dir/${raw_ds}_sub-$sub"
			done
		fi

		datalad save -r

		# Submit jobs via reproman in batches 
		# make sure to 'unlock' outputs
		echo "$all_subs" | xargs -n "$subs_per_job" echo | while read line; do 
			sub_list=$(echo "$line" | sed 's/ /,/g')
			processes=$(echo "$line" | awk '{ print NF }')
			nodes=$(( ($processes + $subs_per_node - 1) / $subs_per_node)) # round up
			if [ "$nodes" -gt 2 ]; then
				queue="normal"
			else
				queue="small"
			fi
			
			reproman run -r local --sub slurm --orc datalad-no-remote \
				--bp sub="$sub_list" --output . \
					--jp num_processes="$processes" --jp num_nodes="$nodes" \
						--jp walltime="$walltime" --jp queue="$queue" --jp launcher=true \
							--jp "container=code/containers/bids-${software}" --jp \
								killjob_factors="$killjob_factors" sourcedata/raw \
									"$derivatives_path" participant --participant-label '{p[sub]}' \
										-w "$work_dir/${raw_ds}_sub-{p[sub]}" -vv "${command[@]}"
			echo
		done
	done
}

# initialize variables
software="$1"
syn_sdc="--use-syn-sdc"
skull_strip="force"
subs_per_job="200"
all_subs_arg=""
subs_per_node=""
skip_raw_download="False"
skip_create_derivatives="False"
skip_run_software="False"
skip_workdir_delete="False"
STAGING="$SCRATCH/openneuro_derivatives"
OPENNEURO="/corral-repl/utexas/poldracklab/data/OpenNeuro/"
work_dir="$SCRATCH/work_dir/$software/"
fs_license=$HOME/.freesurfer.txt # this should be in code/license

# initialize flags
while [[ "$#" > 1 ]]; do
  case $1 in
	--no-syn-sdc)
		syn_sdc="" ;;
	--skull-strip-t1w)
		ss_force=$2; shift ;;
	--sub-list)
		all_subs_arg=$2; shift ;;
	--subs-per-job)
		subs_per_job=$2; shift ;;
	--subs-per-node)
		subs_per_node=$2; shift ;;
	--dataset-list)
		dataset_list=$(cat $2); shift ;;
	--dataset)
		dataset_list=$2; shift ;;
	--skip-raw-download)
		skip_raw_download="True" ;;
	--skip-create-derivatives)
		skip_create_derivatives="True" ;;
	--skip-run-software)
		skip_run_software="True" ;;
	--skip-workdir-delete)
		skip_workdir_delete="True" ;;
  esac
  shift
done


# run full pipeline
# todo: figure out how to run two reproman jobs simultaneously
if [[ "$skip_raw_download" == "False" ]]; then
	download_raw_ds
fi
if [[ "$skip_create_derivatives" == "False" ]]; then
	create_derivatives_ds
fi
if [[ "$skip_run_software" == "False" ]]; then
	run_software
fi

