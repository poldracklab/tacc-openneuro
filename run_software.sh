#!/bin/bash

# Clone/update raw datasets and download necessary data for fmriprep/mriqc
download_raw_ds () {
	local raw_ds="$1"
	local raw_corral_path="$OPENNEURO/raw/$raw_ds"
	
	if [[ ! -d "$raw_corral_path" ]]; then
		datalad clone https://github.com/OpenNeuroDatasets/${raw_ds}.git "$raw_corral_path"

		# Ensure permissions for the group
		setfacl -R -m g:G-802037:rwX "$raw_corral_path"
		find "$raw_corral_path" -type d | xargs setfacl -R -m d:g:G-802037:rwX

		cd "$raw_corral_path" 
		find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
			-exec datalad get {} +
	else
		# Update
		cd "$raw_corral_path"				
		datalad update -s origin --merge
		find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
			-exec datalad get {} +
	fi
}

# Create derivatives dataset if necessary
create_derivatives_ds () {
	local raw_ds="$1"
	local derivatives_corral_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"

	# to-do: fix reckless clone issue when original dataset doesn't download properly 
	if [[ ! -d "$derivatives_corral_path" ]]; then
		datalad create -c yoda "$derivatives_corral_path"
		cd "$derivatives_corral_path"
		git config receive.denyCurrentBranch updateInstead # Allow git pushes to checked out branch
		rm CHANGELOG.md README.md code/README.md
		datalad clone -d . https://github.com/ReproNim/containers.git code/containers
		datalad clone -d . https://github.com/poldracklab/tacc-openneuro.git code/tacc-openneuro
		mkdir sourcedata
		datalad clone -d . https://github.com/OpenNeuroDatasets/"${raw_ds}".git sourcedata/raw
		datalad clone -d . https://github.com/templateflow/templateflow.git sourcedata/templateflow
  
		cp code/tacc-openneuro/gitattributes_openneuro.txt .gitattributes
		cp code/tacc-openneuro/gitattributes_datalad_openneuro.txt .datalad/.gitattributes

		if [[ "$software" == "fmriprep" ]]; then
			# Look for existing freesurfer derivatives
			local fs_path="$OPENNEURO/freesurfer/${raw_ds}-freesurfer"
			if [[ -d "$fs_path" ]]; then
				rsync -tvrL "$fs_path/" "$derivatives_corral_path/sourcedata/freesurfer/"
			fi
		fi
  
		# Ensure permissions for the group
		setfacl -R -m g:G-802037:rwX "$derivatives_corral_path"
		find "$derivatives_corral_path" -type d | xargs setfacl -R -m d:g:G-802037:rwX
		
		datalad save -m "Initialize dataset"
	else
		datalad save -d "$derivatives_corral_path"  
	fi
}

# Recreates datalad dataset in case of purged files
cheap_clone () {
	url="${1%/}"
	loc="${2%/}"

	if [ -d "$loc" ]; then
	    mv "$loc" "$loc.aside"
		git clone "$url" "$loc"
	else
		datalad clone "$url" "$loc"
	fi

	if [ -d "${loc}.aside/.git/annex/objects" ]; then
	    git -C $loc annex init;
	    mkdir -p "$loc/.git/annex/"
	    mv "${loc}.aside/.git/annex/objects" "$loc/.git/annex/"
	    git -C "$loc" annex fsck
	fi

	if [ -d "$loc" ]; then
	    rm -rf "$loc.aside"
	fi

}

# Setup derivatives directory on scratch
setup_scratch_ds () {
	local raw_ds="$1"
	local raw_corral_path="$OPENNEURO/raw/$raw_ds"
	local raw_scratch_path="$STAGING/raw/$raw_ds"
	local derivatives_corral_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	
	cheap_clone "$raw_corral_path" "$raw_scratch_path"
	cd "$raw_scratch_path"
	find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
		-exec datalad get {} +	
	
	cheap_clone "$derivatives_corral_path" "$derivatives_scratch_path"
	cd "$derivatives_scratch_path"
	datalad get .
	datalad clone -d . "$raw_scratch_path" sourcedata/raw --reckless ephemeral
	datalad clone -d . "$STAGING/containers" code/containers --reckless ephemeral
	datalad clone -d . "$STAGING/templateflow" sourcedata/templateflow --reckless ephemeral
	for sub_ds in "$STAGING"/templateflow/tpl*; do
		datalad clone "$sub_ds" sourcedata/templateflow/$(basename "$sub_ds") --reckless ephemeral
	done
}

# Run fmriprep or mriqc
run_software () {
	local raw_ds="$1"
	local derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}" 
	local raw_path="$STAGING/raw/$raw_ds"
	local fs_path="$derivatives_path/sourcedata/freesurfer"
	cd "$derivatives_path"

	if [[ "$software" == "fmriprep" ]]; then
		local walltime="48:00:00"
		local killjob_factors=".85,.25"
		if [ -z "$subs_per_node" ]; then
			local subs_per_node=4
		fi
		local mem_mb="$(( 150000 / $subs_per_node ))"
		local command=("--output-spaces" "MNI152NLin2009cAsym:res-2" "anat" "func" "fsaverage5" "--nthreads" "14" \
			"--omp-nthreads" "7" "--skip-bids-validation" "--notrack" "--fs-license-file" "$fs_license" \
				"--use-aroma" "--ignore" "slicetiming" "--output-layout" "bids" "--cifti-output" "--resource-monitor" \
					"--skull-strip-t1w" "$skull_strip" "--mem_mb" "$mem_mb" "--bids-database-dir" "/tmp" "--md-only-boilerplate")
		if [[ "$syn_sdc" ==  "True" ]]; then
			command+=("--use-syn-sdc")
			command+=("warn")
		fi
		if [[ -d "$fs_path" ]]; then
			datalad unlock "$fs_path"/sub*/scripts/
			find "$fs_path" -name "*IsRunning*" -delete
			git commit -m "unlock freesurfer scripts"
		fi
		
	elif [[ "$software" == "mriqc" ]]; then
		local walltime="8:00:00"
		local killjob_factors=".85,.25"
		if [ -z "$subs_per_node" ]; then
			local subs_per_node=5
		fi
		local mem_mb="$(( 150 / $subs_per_node ))"
		local command=("--nprocs" "11" "--ants-nthreads" "8" "--verbose-reports" "--dsname" "$raw_ds" "--ica" "--mem_gb" "$mem_mb")
	fi

	if [ -z "$all_subs_arg" ]; then
		if [[ "$rerun" == "True" ]]; then
			unset failed_joined
			check_results "$raw_ds"
			local all_subs=$( echo "$failed_joined" | sed 's/,/\n/g')
		elif [[ "$remaining" == "True" ]]; then
			unset sub_joined
			check_results "$raw_ds"
			local all_subs=$( echo "$sub_joined" | sed 's/,/\n/g')
		else
			local all_subs=$(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' | sort)
		fi
	else
		local all_subs=$(echo "$all_subs_arg" | sed 's/,/\n/g')
	fi
	
	# Remove old work dirs
	if [[ "$skip_workdir_delete" == "False" ]]; then
		for sub in $all_subs; do
			rm -rf "$work_dir/${raw_ds}_sub-$sub"
		done
	fi

	if [[ "$rerun" == "True" ]]; then
		cd "$derivatives_path/code/containers"
		git-annex repair --force
		cd "$derivatives_path"
		for sub in $all_subs; do
			rm -rf "$derivatives_path/sub-${sub}"*
		done
		if [[ "$software" == "fmriprep" ]]; then
			rm -rf "$derivatives_path"/sourcedata/freesurfer/fsaverage*
			rsync -tvrL "$OPENNEURO"/freesurfer/ds000001-freesurfer/fsaverage* sourcedata/freesurfer/
		fi
	fi
	
	datalad save -r -m "pre-run save"

	export SINGULARITYENV_TEMPLATEFLOW_HOME="$derivatives_path/sourcedata/templateflow/"
	export SINGULARITYENV_TEMPLATEFLOW_USE_DATALAD="true"
	# Submit jobs via reproman in batches 
	local count=0
	echo "$all_subs" | xargs -n "$subs_per_job" echo | while read line; do 
		((count++))

		if [ "$part" != "$count" ]; then
			continue
		fi

		local sub_list=$(echo "$line" | sed 's/ /,/g')
		local processes=$(echo "$line" | awk '{ print NF }')
		local nodes=$(( ($processes + $subs_per_node - 1) / $subs_per_node)) # round up
		if [ "$nodes" -gt 2 ]; then
			local queue="normal"
		else
			local queue="small"
		fi
		
		reproman run -r local --sub slurm --orc datalad-no-remote \
			--bp sub="$sub_list" \
				--jp num_processes="$processes" --jp num_nodes="$nodes" \
					--jp walltime="$walltime" --jp queue="$queue" --jp launcher=true \
						--jp job_name="${raw_ds}-${software}" --jp mail_type=END --jp mail_user="$user_email" \
							--jp "container=code/containers/bids-${software}" --jp \
								killjob_factors="$killjob_factors" sourcedata/raw \
									"$derivatives_path" participant --participant-label '{p[sub]}' \
										-w "$work_dir/${raw_ds}_sub-{p[sub]}" -vv "${command[@]}"
		echo
	done
}

convertsecs () {
 ((h=${1}/3600))
 ((m=(${1}%3600)/60))
 ((s=${1}%60))
 printf "%02d:%02d:%02d\n" $h $m $s
}

check_results () {
	local raw_ds="$1"
	local derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	local derivatives_path_corral="$OPENNEURO/$software/${raw_ds}-${software}"
	local raw_path="$OPENNEURO/raw/$raw_ds"
	
	if [ -z "$success_array" ]; then
		success_array=()	
	fi
	if [ -z "$fail_array" ]; then
		fail_array=()
	fi
	if [ -z "$incomplete_array" ]; then
		incomplete_array=()
	fi
	
	if [[ "$software" == "mriqc" ]]; then
		local success_phrase="MRIQC completed"
	elif [[ "$software" == "fmriprep" ]]; then
		local success_phrase="fMRIPrep finished successfully"
	fi
	local reproman_logs="$(ls -1d $derivatives_path/.reproman/jobs/local/* | sort -nr)"
	local sub_array
	readarray -t sub_array < <(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' | sort )
	local success_sub_array=()
	local failed_sub_array=()
	local incomplete_sub_array=()
	local runtime_array=()
	
	while IFS= read -r job_dir && [ ${#sub_array[@]} -gt 0 ]; do		
		echo "$job_dir"
		for stdout in "$job_dir"/stdout.*; do
			local stderr=$(echo "$stdout" | sed 's/stdout/stderr/g')
			local sub=$(head -n 10 "$stdout" | grep "\-\-participant-label" | sed -r 's/.*--participant-label \x27([^\x27]*)\x27.*/\1/g'01)
			# Look for exact match in array
			if [[ ${sub_array[*]} =~ (^|[[:space:]])"$sub"($|[[:space:]]) ]]; then
				# Remove sub from array
				for i in "${!sub_array[@]}";do
					if [[ "${sub_array[$i]}" == "$sub" ]];then 
						unset 'sub_array[i]'
						break
					fi
				done
				if ! grep -q "$success_phrase" "$stdout" || grep -q "did not finish successfully" "$stdout" || grep -q "Error" "$stdout"; then
					echo "$stdout (sub-$sub) failed "
					failed_sub_array+=("$sub")
				elif grep -q "Error" "$stderr"; then
					echo "$stderr (sub-$sub) failed"
					failed_sub_array+=("$sub")
				else
					success_sub_array+=("$sub")
				fi
				
				# get runtime
				start_time=$(head "$stdout" | sed -rn 's|.*([0-9]{2})([0-9]{2})([0-9]{2})-([0-9]{2}):([0-9]{2}):([0-9]{2}),.*|20\1-\2-\3 \4:\5:\6|p' )
				end_time=$(tail -20 "$stdout" | sed -rn 's|.*([0-9]{2})([0-9]{2})([0-9]{2})-([0-9]{2}):([0-9]{2}):([0-9]{2}),.*|20\1-\2-\3 \4:\5:\6|p' | tail -n1 )
				start_sec=$(date --date "$start_time" +%s)
				end_sec=$(date --date "$end_time" +%s)
				delta_sec=$((end_sec - start_sec))
				delta=$(convertsecs $delta_sec)
				sub_status=$(cat $(echo $stdout | sed 's/stdout/status/g'))
				runtime_array+=("sub-${sub}: $delta $sub_status")
				
			fi
		done
	done <<< "$reproman_logs"
	
	if [ ${#success_sub_array[@]} -gt 0 ]; then
		local success_joined
		printf -v success_joined '%s,' "${success_sub_array[@]}"
		if [[ "$check" == "True" ]]; then
			echo "The following subjects were successful: "
			echo "${success_joined%,}"
		fi
	fi
	
	if [ ${#failed_sub_array[@]} -gt 0 ]; then
		local fail="True"
		printf -v failed_joined '%s,' "${failed_sub_array[@]}"
		if [[ "$check" == "True" ]]; then
			echo "The following subjects failed: "
			echo "${failed_joined%,}"
		fi
	fi
	
	# Check all subject directories exist
	local raw_sub_array derivatives_sub_array
	readarray -t raw_sub_array < <(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' )
	readarray -t derivatives_sub_array < <(find "$derivatives_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' )
	local unique_array=($(comm -3 <(printf "%s\n" "${raw_sub_array[@]}" | sort) <(printf "%s\n" "${derivatives_sub_array[@]}" | sort) | sort -n)) # print unique elements
	if [ ${#unique_array[@]} -gt 0 ]; then
		local incomplete="True"
		local unique_joined
		printf -v unique_joined '%s,' "${unique_array[@]}"
		if [[ "$check" == "True" ]]; then
			echo "The following subjects dirs do not exist: "
			echo "${unique_joined%,}"
		fi
	fi
	
	if [ ${#sub_array[@]} -gt 0 ]; then
		local incomplete="True"
		printf -v sub_joined '%s,' "${sub_array[@]}"
		if [[ "$check" == "True" ]]; then
			echo "The following subjects have not been run: "
			echo "${sub_joined%,}"
		fi
	fi
	
	total_run_subs=$(bc -l <<< "( ${#success_sub_array[@]} + ${#failed_sub_array[@]} )" )
	if [ ${#success_sub_array[@]} -eq 0 ]; then
		success_percent=0
	elif [ ${#failed_sub_array[@]} -eq 0 ]; then
		local fail="False" 
		success_percent=100
	else
		success_percent=$(bc -l <<< "scale = 10; ( ${#success_sub_array[@]} / $total_run_subs ) * 100" )
	fi
	echo "${success_percent:0:4}% (${#success_sub_array[@]}/$total_run_subs) of attempted subjects were successful."
	
	printf '%s\n' "${runtime_array[@]}"
	
	if [[ "$fail" == "True" ]]; then
		fail_array+=("$raw_ds")
	elif [[ "$incomplete" != "True" ]]; then
		success_array+=("$raw_ds")
	else
		incomplete_array+=("$raw_ds")
	fi		
}

clone_derivatives () {
	local raw_ds="$1"
	local derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	local derivatives_path_corral="$OPENNEURO/$software/${raw_ds}-${software}"
	local raw_path="$STAGING/raw/$raw_ds"
	
	# Move remora logs to corral
	datalad unlock -d "$derivatives_path" "$derivatives_path"/remora*
	mkdir "$OPENNEURO/logs/$software/remora/${raw_ds}-${software}-remora/"
	mv "$derivatives_path"/remora* "$OPENNEURO/logs/$software/remora/${raw_ds}-${software}-remora/"
	datalad save -d "$derivatives_path" -m "remove remora logs from ds"
	
	datalad clone "$derivatives_path" "$derivatives_path_corral"
	cd "$derivatives_path_corral/$ds"
	datalad get .
	git config --file .gitmodules --replace-all submodule.code/containers.url https://github.com/ReproNim/containers.git
	git config --file .gitmodules --unset-all submodule.code/containers.datalad-url
	git config --file .gitmodules --replace-all submodule.sourcedata/raw.url https://github.com/OpenNeuroDatasets/"$raw_ds".git
	git config --file .gitmodules --unset-all submodule.sourcedata/raw.datalad-url
	git config --file .gitmodules --unset-all submodule.sourcedata/templateflow.datalad-url
	git-annex lock
	datalad save -r -m "change gitmodule urls to origin"
	datalad install . -r
	
	local derivatives_path_old="$STAGING/derivatives/$software/old/${raw_ds}-${software}"
	if [[ -d "$derivatives_path_old" ]]; then
		chmod -R 775 "$derivatives_path_old"
		rm -rf "$derivatives_path_old"
	fi
	mv -f "$derivatives_path" "$derivatives_path_old"
	rm -rf "$SCRATCH/work_dir/$software/$raw_ds"*
}


# initialize variables
user_email="jbwexler@tutanota.com"
software="$1"
syn_sdc="True"
skull_strip="force"
subs_per_job="100"
all_subs_arg=""
subs_per_node=""
skip_raw_download="False"
skip_create_derivatives="False"
skip_run_software="False"
skip_workdir_delete="False"
download_create_run="True"
part="1"
STAGING="$SCRATCH/openneuro_derivatives"
OPENNEURO="/corral-repl/utexas/poldracklab/data/OpenNeuro/"
work_dir="$SCRATCH/work_dir/$software/"
fs_license=$HOME/.freesurfer.txt # this should be in code/license

# initialize flags
while [[ "$#" > 0 ]]; do
  case $1 in
	--no-syn-sdc)
		syn_sdc="False" ;;
	--skull-strip-t1w)
		skull_strip=$2; shift ;;
	--sub-list)
		all_subs_arg=$2; shift ;;
	--subs-per-job)
		subs_per_job=$2; shift ;;
	--subs-per-node)
		subs_per_node=$2; shift ;;
	--dataset-file)
		dataset_list=$(cat $2); shift ;;
	--dataset)
		dataset_list=$(echo $2 | sed 's/,/\n/g'); shift ;;
	--dataset-all)
		dataset_list=$(find "$STAGING"/derivatives/"$software"/ -name "ds*" -maxdepth 1 | sed -r 's/.*(ds......).*/\1/g') ;;
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
	--remaining)
		subs_per_job="2000"
		remaining="True" ;;
	--rerun)
		skip_workdir_delete="True"
		rerun="True" ;;
	--check)
		check="True"
		clone_derivatives="True"
		download_create_run="False" ;;
	--part)
		part=$2; shift ;;
	-x)
		set -x; shift ;;
  esac
  shift
done

if [ -z "$dataset_list" ]; then
	echo "No datasets list provided"
	exit 1
fi

# run full pipeline
if [[ "$download_create_run" == "True" ]]; then
	if [[ "$skip_raw_download" == "False" ]]; then
		while IFS= read -r raw_ds; do  
			download_raw_ds "$raw_ds"
		done <<< "$dataset_list"
	fi
	if [[ "$skip_create_derivatives" == "False" ]]; then
		rsync -av "$OPENNEURO/software/containers" "$STAGING"
		rsync -av "$OPENNEURO/software/templateflow" "$STAGING"
		while IFS= read -r raw_ds; do  
			create_derivatives_ds "$raw_ds"
		done <<< "$dataset_list"	
	fi
	if [[ "$skip_run_software" == "False" ]]; then
		while IFS= read -r raw_ds; do  
			setup_scratch_ds "$raw_ds"
			run_software "$raw_ds"
		done <<< "$dataset_list"		
	fi	
elif [[ "$clone_derivatives" == "True" ]]; then
	while IFS= read -r raw_ds; do  
		check_results "$raw_ds"
	done <<< "$dataset_list"
	printf -v success_print "%s," "${success_array[@]}"
	printf -v failed_print "%s," "${fail_array[@]}"
	printf -v incomplete_print "%s," "${incomplete_array[@]}"
	echo -e "\nSuccess: "
	echo "${success_print%,}"
	echo -e "Failed: "
	echo "${failed_print%,}"
	echo -e "Incomplete: "
	echo "${incomplete_print%,}"
	
	if [[ "$check" != "True" ]]; then
		while IFS= read -r raw_ds; do  
			clone_derivatives "$raw_ds" 
		done <<< "$(echo ${success_print%,} | sed 's/,/\n/g')"
	fi
fi
