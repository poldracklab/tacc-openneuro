#!/bin/bash


# Clone/update raw datasets and download necessary data for freesurfer/fmriprep/mriqc
download_raw_ds () {
	while IFS= read -r raw_ds; do  
		raw_path="$STAGING/raw/$raw_ds"
		
		if [[ -d "$raw_path" ]] && [[ ! -f "$raw_path/dataset_description.json" ]]; then
			# Delete datasets on $SCRATCH that have been purged by TACC
			rm -rf "$raw_path"
		fi
		
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
	done <<< "$dataset_list"
}

# Create derivatives dataset if necessary
create_derivatives_ds () {
	while IFS= read -r raw_ds; do  
		derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
		raw_path="$STAGING/raw/$raw_ds"

		# To-do: fix reckless clones so they link to original URL; add line to download licenses; fix reckless clone issue when original dataset doesn't download properly 
		if [[ ! -d "$derivatives_path" ]]; then
			datalad create -c yoda "$derivatives_path"
			cd "$derivatives_path"
			rm CHANGELOG.md README.md code/README.md
			datalad clone -d . "$STAGING/containers" code/containers --reckless ephemeral
			git clone https://github.com/poldracklab/tacc-openneuro.git code/tacc-openneuro
			mkdir sourcedata
			datalad clone -d . "$raw_path" sourcedata/raw --reckless ephemeral
	  
			cp code/tacc-openneuro/gitattributes_openneuro.txt .gitattributes
			cp code/tacc-openneuro/gitattributes_datalad_openneuro.txt .datalad/.gitattributes

			if [[ "$software" == "fmriprep" ]]; then
				# Look for existing freesurfer derivatives
				fs_path="$OPENNEURO/freesurfer/${raw_ds}-freesurfer"
				if [[ -d "$fs_path" ]]; then
					rsync -tvrL "$fs_path/" "$derivatives_path/sourcedata/freesurfer/"
				fi
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
	done <<< "$dataset_list"
}

# Run fmriprep or mriqc
run_software () {
	while IFS= read -r raw_ds; do  
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
		count=0
		echo "$all_subs" | xargs -n "$subs_per_job" echo | while read line; do 
			((count++))
			if [ -n "$part" ]; then
				if [ "$part" != "$count" ]; then
					continue
				fi
			fi	
				
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
							--jp job_name="${raw_ds}-${software}" --jp mail_type=END --jp mail_user="$user_email" \
								--jp "container=code/containers/bids-${software}" --jp \
									killjob_factors="$killjob_factors" sourcedata/raw \
										"$derivatives_path" participant --participant-label '{p[sub]}' \
											-w "$work_dir/${raw_ds}_sub-{p[sub]}" -vv "${command[@]}"
			echo
		done
	done <<< "$dataset_list"
}

clone_derivatives () {
	success_array=()
	while IFS= read -r raw_ds; do  
		derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
		derivatives_path_corral="$OPENNEURO/$software/${raw_ds}-${software}"
		
		reproman_logs="$derivatives_path/.reproman/jobs/local/$(ls -1 $derivatives_path/.reproman/jobs/local/ | tail -n 1)"
		fail="False"
		for f in "$reproman_logs"/status.*; do
			status="$(cat $f)"
			if [[ "$status" != "succeeded" ]]; then
				echo "$f failed"
				fail="True"
			fi
		done
		if [[ "$software" == "mriqc" ]]; then
			success_phrase="MRIQC completed"
		elif [[ "$software" == "fmriprep" ]]; then
			success_phrase="fMRIPrep finished successfully"
		fi
		for f in "$reproman_logs"/stdout.*; do
			if [[ "$(tail -n 10 $f)" != *"$success_phrase"* ]]; then
				echo "$f failed"
				fail="True"
			fi
		done
		
		if [[ "$fail" == "True" ]]; then
			success_array+=("${raw_ds}: failed")
			if [[ "$ignore" != "True" ]]; then
				continue
			fi
		else	
			success_array+=("${raw_ds}: success")
		fi		
		
		if [[ "$check" == "True" ]]; then
			continue
		fi
		
		mv "$derivatives_path"/remora* "$OPENNEURO/logs/remora/${raw_ds}-${software}-remora"
		datalad save -r -d "$derivatives_path"
		
		datalad clone "$derivatives_path" "$derivatives_path_corral"
		cd "$derivatives_path_corral/$ds"
		datalad get sub* -r
		git config --file .gitmodules --replace-all submodule.code/containers.url https://github.com/ReproNim/containers.git
		git config --file .gitmodules --unset-all submodule.code/containers.datalad-url https://github.com/ReproNim/containers.git
		git config --file .gitmodules --replace-all submodule.sourcedata/raw.url https://github.com/OpenNeuroDatasets/"$raw_ds".git
		git config --file .gitmodules --unset-all submodule.sourcedata/raw.datalad-url https://github.com/OpenNeuroDatasets/"$raw_ds".git
		datalad install . -r
		
		derivatives_path_old="$STAGING/derivatives/$software/old/${raw_ds}-${software}"
		datalad remove -d "$derivatives_path_old" --nocheck -r
		rm -rf "$derivatives_path_old"
		mv -f "$derivatives_path" "$derivatives_path_old"
	done <<< "$dataset_list"
	echo
	printf "%s\n" "${success_array[@]}"
}


# initialize variables
user_email="jbwexler@tutanota.com"
software="$1"
syn_sdc="--use-syn-sdc"
skull_strip="force"
subs_per_job="100"
all_subs_arg=""
subs_per_node=""
skip_raw_download="False"
skip_create_derivatives="False"
skip_run_software="False"
skip_workdir_delete="False"
download_create_run="True"
STAGING="$SCRATCH/openneuro_derivatives"
OPENNEURO="/corral-repl/utexas/poldracklab/data/OpenNeuro/"
work_dir="$SCRATCH/work_dir/$software/"
fs_license=$HOME/.freesurfer.txt # this should be in code/license

# initialize flags
while [[ "$#" > 0 ]]; do
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
	--clone)
		clone_derivatives="True"
		download_create_run="False" ;;
	--ignore)
		ignore="True" ;;
	--check)
		check="True"
		clone_derivatives="True"
		download_create_run="False" ;;
	--part)
		part=$2; shift ;;
  esac
  shift
done

if [ -z "$dataset_list" ]; then
	echo "No datasets list provided"
	exit 1
fi

# run full pipeline
# todo: figure out how to run two reproman jobs simultaneously
if [[ "$download_create_run" == "True" ]]; then
	if [[ "$skip_raw_download" == "False" ]]; then
		download_raw_ds
	fi
	if [[ "$skip_create_derivatives" == "False" ]]; then
		create_derivatives_ds
	fi
	if [[ "$skip_run_software" == "False" ]]; then
		run_software
	fi
elif [[ "$clone_derivatives" == "True" ]]; then
	clone_derivatives
fi
	
