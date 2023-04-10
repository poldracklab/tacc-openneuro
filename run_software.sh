#!/bin/bash

set -eu

get_subs () {
	local raw_ds="$1"
	local raw_corral_path="$OPENNEURO/raw/$raw_ds"
	all_subs_temp=''
	declare -ag all_subs_arr=()
	if [ -z "$all_subs_arg" ]; then
		if [[ "$rerun" == "True" ]]; then
			unset failed_joined
			check_results "$raw_ds"
			all_subs_temp="${failed_joined[@]//,/$'\n'}"
		elif [[ "$remaining" == "True" ]]; then
			unset sub_joined
			check_results "$raw_ds"
			all_subs_temp="${sub_joined//,/$'\n'}"
		else
			all_subs_temp=$(find "$raw_corral_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' | sort)
		fi
	else
		all_subs_temp="${all_subs_arg//,/$'\n'}"
	fi

	if [[ "$software" == "mriqc" ]]; then
		while read -r sub; do
			if [[ $(find "$raw_corral_path/sub-${sub}/" -wholename "*anat/*.nii*" -o -wholename "*func/*.bold.nii*") ]]; then
				all_subs_arr+=("$sub")
				printf -v all_subs '%s,' "${all_subs_arr[@]}"
			fi
		done <<< "$all_subs_temp"
	elif [[ "$software" == "fmriprep" ]]; then
               	while read -r sub; do
                        if [[ $(find "$raw_corral_path/sub-${sub}/" -wholename "*func/*bold.nii*") ]] && [[ $(find "$raw_corral_path/sub-${sub}/" -wholename "*anat/*T1w.nii*") ]]; then
                                all_subs_arr+=("$sub")
			fi
		done <<< "$all_subs_temp"
	fi

	printf -v all_subs '%s ' "${all_subs_arr[@]}"
}

# Clone/update raw datasets and download necessary data for fmriprep/mriqc
download_raw_ds () {
	local raw_ds="$1"
	local raw_corral_path="$OPENNEURO/raw/$raw_ds"
	
	if [[ ! -d "$raw_corral_path" ]]; then
		datalad clone https://github.com/OpenNeuroDatasets/"${raw_ds}".git "$raw_corral_path"

		# Ensure permissions for the group
		setfacl -R -m g:G-802037:rwX "$raw_corral_path"
		find "$raw_corral_path" -type d -print0 | xargs --null setfacl -R -m d:g:G-802037:rwX

		cd "$raw_corral_path" || exit
		find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
			-exec datalad get {} +
		git annex fsck
	else
		# Update
		cd "$raw_corral_path" || exit			
		datalad update -s origin --merge
		find sub-*/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
			-exec datalad get {} + 
	fi
}

# Create derivatives dataset if necessary
create_derivatives_ds () {
	local raw_ds="$1"
	local derivatives_inprocess_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}"

	# to-do: fix reckless clone issue when original dataset doesn't download properly 
	if [[ ! -d "$derivatives_inprocess_path" ]]; then
		datalad create -c yoda "$derivatives_inprocess_path"
		cd "$derivatives_inprocess_path" || exit
		git config receive.denyCurrentBranch updateInstead # Allow git pushes to checked out branch
		rm CHANGELOG.md README.md code/README.md
		
		datalad clone -d . https://github.com/ReproNim/containers.git code/containers
		datalad clone -d . https://github.com/poldracklab/tacc-openneuro.git code/tacc-openneuro
		mkdir sourcedata
		datalad clone -d . https://github.com/OpenNeuroDatasets/"${raw_ds}".git sourcedata/raw
		datalad clone -d . https://github.com/templateflow/templateflow.git sourcedata/templateflow
  
		cp code/tacc-openneuro/README_"${software}".md README.md
		cp code/tacc-openneuro/gitattributes_openneuro.txt .gitattributes
		cp code/tacc-openneuro/gitattributes_datalad_openneuro.txt .datalad/.gitattributes
		sed -i "s/ds000000/${raw_ds:0:8}/g" README.md
		

		#if [[ "$software" == "fmriprep" ]]; then
			# Look for existing freesurfer derivatives
		#	local fs_path="$OPENNEURO/freesurfer/${raw_ds}-freesurfer"
		#	if [[ -d "$fs_path" ]]; then
		#		rsync -tvrL "$fs_path/" "$derivatives_inprocess_path/sourcedata/freesurfer/"
		#	else
		#		rsync -tvrL "$OPENNEURO"/freesurfer/ds000001-freesurfer/fsaverage* "$derivatives_inprocess_path/sourcedata/freesurfer/"
		#	fi
		#	find "$derivatives_inprocess_path"/sourcedata/freesurfer/fsaverage* -exec touch -h {} + 
		#fi
  
		# Ensure permissions for the group
		setfacl -R -m g:G-802037:rwX "$derivatives_inprocess_path"
		find "$derivatives_inprocess_path" -type d -print0 | xargs --null setfacl -R -m d:g:G-802037:rwX
		
		datalad siblings add -s scratch --url "$derivatives_scratch_path"
		datalad save -m "Initialize dataset"
	else
		datalad save -d "$derivatives_inprocess_path" -r -m "ensure in_process copy is clean"
	fi
}

# Recreates datalad dataset in case of purged files
cheap_clone () {
	url="${1%/}"
	loc="${2%/}"

	if [ -d "$loc" ]; then
	    mv "$loc" "$loc.aside"
		git clone "$url" "$loc"
		if [ -d "${loc}.aside/.git/annex/objects" ]; then
		    git -C "$loc" annex init;
		    mkdir -p "$loc/.git/annex/"
		    mv "${loc}.aside/.git/annex/objects" "$loc/.git/annex/"
		    git -C "$loc" annex fsck
		fi
		if [ -d "$loc" ]; then
			chmod -R 775 "$loc.aside"
		    rm -rf "$loc.aside"
		fi
	else
		datalad clone "$url" "$loc"
	fi
}

# Setup derivatives directory on scratch
setup_scratch_ds () {
	local raw_ds="$1"
	local raw_corral_path="$OPENNEURO/raw/$raw_ds"
	local raw_scratch_path="$STAGING/raw/$raw_ds"
	local derivatives_inprocess_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	
	if [[ "$rerun" == "True" ]]; then
		push "$raw_ds"
	fi
	check_corral="True"
	get_subs "$raw_ds"
	
	datalad save -d "$derivatives_inprocess_path" -m "pre-setup_scratch_ds"
	if [[ "$skip_raw_clone" == "True" ]]; then
		datalad update -d "$raw_scratch_path" -s origin --merge	
	else
		cheap_clone "$raw_corral_path" "$raw_scratch_path"
	fi
	cd "$raw_scratch_path" || exit
	for sub in $all_subs; do
		if [ -d "sub-${sub}" ]; then
			find "sub-${sub}"/ -regex ".*_\(T1w\|T2w\|bold\|sbref\|magnitude.*\|phase.*\|fieldmap\|epi\|FLAIR\|roi\)\.nii\(\.gz\)?" \
				-exec datalad get {} +	
		fi
	done
	
	cheap_clone "$derivatives_inprocess_path" "$derivatives_scratch_path"
	cd "$derivatives_scratch_path" || exit
	if [[ -d "sourcedata/freesurfer/fsaverage" ]]; then
		datalad get sourcedata/freesurfer/fsaverage*
	fi
	if [[ -d ".reproman" ]]; then
		datalad get .reproman
	fi
	for sub in $all_subs; do
		if [ -d sourcedata/freesurfer/sub-"${sub}" ]; then
			datalad get sourcedata/freesurfer/sub-"${sub}"
		fi
	done
	datalad clone -d . --reckless ephemeral "$raw_scratch_path" sourcedata/raw
	if [[ "$freesurfer_6" == "True" ]]; then
		datalad clone -d . --reckless ephemeral "$STAGING/containers_freesurfer6" code/containers
	else
		datalad clone -d . --reckless ephemeral "$STAGING/containers" code/containers
	fi
	datalad clone -d . --reckless ephemeral "$STAGING/templateflow" sourcedata/templateflow
	for sub_ds in "$STAGING"/templateflow/tpl*; do
		datalad clone  --reckless ephemeral -d . "$sub_ds" sourcedata/templateflow/"$(basename "$sub_ds")"
	done
}

# Run fmriprep or mriqc
run_software () {
	local raw_ds="$1"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}" 
	local raw_path="$STAGING/raw/$raw_ds"
	local fs_path="$derivatives_scratch_path/sourcedata/freesurfer"
	get_subs "$raw_ds"
	
	cd "$derivatives_scratch_path" || exit

	if [[ "$software" == "fmriprep" ]]; then
		local killjob_factors=".85,.25"
		if [ -z "$subs_per_node" ]; then
			local subs_per_node=4
		fi
		local mem_mb="$(( 150000 / subs_per_node ))"
		local command=("--output-spaces" "MNI152NLin2009cAsym:res-2" "anat" "func" "fsaverage5" "--nthreads" "14" \
			"--omp-nthreads" "7" "--skip-bids-validation" "--notrack" "--fs-license-file" "$fs_license" \
			"--me-output-echos" \
			"--use-aroma" "--ignore" "slicetiming" "--output-layout" "bids" "--cifti-output" \
			"--skull-strip-t1w" "$skull_strip" "--mem_mb" "$mem_mb" "--bids-database-dir" "/tmp" "--md-only-boilerplate")
		if [[ "$syn_sdc" ==  "True" ]]; then
			command+=("--use-syn-sdc")
			command+=("warn")
		fi
		
	elif [[ "$software" == "mriqc" ]]; then
		local killjob_factors=".85,.25"
		if [ -z "$subs_per_node" ]; then
			local subs_per_node=5
		fi
		local mem_mb="$(( 150 / subs_per_node ))"
		local command=("--nprocs" "11" "--ants-nthreads" "8" "--verbose-reports" "--dsname" "$raw_ds" "--ica" "--mem_gb" "$mem_mb")
	fi
	
	if [[ -d "$fs_path" ]]; then
		for sub in $all_subs; do
			if [[ -d "$fs_path/sub-${sub}" ]]; then
				datalad unlock "$fs_path/sub-${sub}"
				find "$fs_path/sub-${sub}" -name "*IsRunning*" -delete
			fi
		done
		git add -A
		git diff-index --quiet HEAD || git commit -m "unlock freesurfer"
	fi

	cd "$derivatives_scratch_path" || exit
	for sub in $all_subs; do
		if [[ -d "$derivatives_scratch_path/sub-${sub}" ]]; then
			rm -rf "$derivatives_scratch_path/sub-${sub}"
		fi
		if [[ -L "$derivatives_scratch_path/sub-${sub}.html" ]] || [[ -f "$derivatives_scratch_path/sub-${sub}.html" ]]; then
			rm -rf "$derivatives_scratch_path/sub-${sub}.html"
		fi
		if [[ -f "$work_dir/${raw_ds}_sub-${sub}".tar ]]; then
			tar -xvf "$work_dir/${raw_ds}_sub-${sub}".tar -C /  && rm -rf "$work_dir/${raw_ds}_sub-${sub}".tar
			find "$work_dir/${raw_ds}_sub-${sub}" -type f -exec touch {} +
		fi
	done
	
	datalad save -r -m "pre-run save"

	export SINGULARITYENV_TEMPLATEFLOW_HOME="$derivatives_scratch_path/sourcedata/templateflow/"
	export SINGULARITYENV_TEMPLATEFLOW_USE_DATALAD="true"
	# Submit jobs via reproman in batches 
	local count=0
	echo "$all_subs" | xargs -n "$subs_per_job" echo | while read -r line; do 
		(( ++count ))
		if [ "$part" != "$count" ]; then
			continue
		fi

		local sub_list="${line// /,}"
		local processes; processes=$(echo "$line" | awk '{ print NF }')
		local nodes=$(( (processes + subs_per_node - 1) / subs_per_node)) # round up
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
									"$derivatives_scratch_path" participant --participant-label '{p[sub]}' \
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

push () {
	local raw_ds="$1"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	local derivatives_inprocess_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"
	find "$derivatives_scratch_path" -name ".proc*" -type f -delete
	datalad save -d "$derivatives_scratch_path" -m "pre-push save (scratch)"
    datalad save -d "$derivatives_inprocess_path" -m "pre-push save (corral)"
	datalad update --merge -d "$derivatives_inprocess_path" -s scratch
    datalad update --merge -d "$derivatives_scratch_path" -s origin
	datalad push --to origin -d "$derivatives_scratch_path"
}

check_results () {
	local raw_ds="$1"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	local derivatives_inprocess_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"
	local raw_path="$OPENNEURO/raw/$raw_ds"
	
	if [ -z "${success_array+x}" ]; then
		declare -ag success_array=()
	fi
	if [ -z "${fail_array+x}" ]; then
		declare -ag fail_array=()
	fi
	if [ -z "${error_array+x}" ]; then
		declare -ag error_array=()
	fi
	if [ -z "${incomplete_array+x}" ]; then
		declare -ag incomplete_array=()
	fi
	if [ -z "${success_sub_count+x}" ]; then
		success_sub_count=0
	fi
	if [ -z "${error_sub_count+x}" ]; then
		error_sub_count=0
	fi
	
	if [[ "$software" == "mriqc" ]]; then
		local success_phrase="MRIQC completed"
	elif [[ "$software" == "fmriprep" ]]; then
		local success_phrase="fMRIPrep finished successfully"
	fi
	
	local reproman_logs
	if [[ "$check_corral" == "True" ]]; then
		reproman_logs="$(find "$derivatives_inprocess_path/.reproman/jobs/local/" -maxdepth 1 -mindepth 1 | sort -nr)"
	else
		reproman_logs="$(find "$derivatives_scratch_path/.reproman/jobs/local/" -maxdepth 1 -mindepth 1 | sort -nr)"
	fi
	local sub_array
	readarray -t sub_array < <(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' | sort )
	local success_sub_array=()
	local failed_sub_array=()
	local error_sub_array=()
	local runtime_array=()
	
	while IFS= read -r job_dir && [ ${#sub_array[@]} -gt 0 ]; do		
		echo "$job_dir"
		for stdout in "$job_dir"/stdout.*; do
			local stderr="${stdout//stdout/stderr}"
			local sub; sub=$(head -n 10 "$stdout" | grep "\-\-participant-label" | sed -r 's/.*--participant-label \x27([^\x27]*)\x27.*/\1/g')
			# Look for exact match in array
			if [[ ${sub_array[@]+"${sub_array[@]}"} =~ (^|[[:space:]])"$sub"($|[[:space:]]) ]]; then
				# Remove sub from array
				for i in "${!sub_array[@]}";do
					if [[ "${sub_array[$i]}" == "$sub" ]];then 
						unset 'sub_array[i]'
						break
					fi
				done
				if ! grep -q "$success_phrase" "$stdout" || grep -q "did not finish successfully" "$stdout"; then
					echo "$stdout (sub-$sub) failed "
					failed_sub_array+=("$sub")
				elif grep -iq "Error" "$stderr"; then
					echo "$stderr (sub-$sub) contains errors"
					if [[ "$errors" == "True" ]]; then
						grep -i "Error" "$stderr" | awk '!x[$0]++'
					fi
					error_sub_array+=("$sub")
				elif grep "Error" "$stdout" | grep -v "Proxy Error" | grep -v "Error reading from remote server" | grep -qv "Internal Server Error"; then
					echo "$stdout (sub-$sub) contains errors "
					if [[ "$errors" == "True" ]]; then
						grep "Error" "$stdout" | awk '!x[$0]++'
					fi
					error_sub_array+=("$sub")
				else
					success_sub_array+=("$sub")
				fi
				
				# get runtime
				local start_time; start_time=$(head "$stdout" | sed -rn 's|.*([0-9]{2})([0-9]{2})([0-9]{2})-([0-9]{2}):([0-9]{2}):([0-9]{2}),.*|20\1-\2-\3 \4:\5:\6|p' )
				local end_time; end_time=$(tail -20 "$stdout" | sed -rn 's|.*([0-9]{2})([0-9]{2})([0-9]{2})-([0-9]{2}):([0-9]{2}):([0-9]{2}),.*|20\1-\2-\3 \4:\5:\6|p' | tail -n1 )
				local start_sec; start_sec=$(date --date "$start_time" +%s)
				local end_sec; end_sec=$(date --date "$end_time" +%s)
				local delta_sec; delta_sec=$((end_sec - start_sec))
				local delta; delta=$(convertsecs $delta_sec)
				local sub_status; sub_status=$(cat "${stdout//stdout/status}")
				runtime_array+=("sub-${sub}: $delta $sub_status")
				
			fi
		done
		echo
	done <<< "$reproman_logs"
	

        if [[ "$ignore_errors" == "True" ]] && [ ${#error_sub_array[@]} -gt 0 ]; then
                if [ ${#success_sub_array[@]} -gt 0 ]; then
                        success_sub_array=("${success_sub_array[@]}" "${error_sub_array[@]}")
                else
                    	success_sub_array=("${error_sub_array[@]}")
                fi
        fi

	if [[ "$errors" == "True" ]] && [ ${#error_sub_array[@]} -gt 0 ]; then
                if [ ${#failed_sub_array[@]} -gt 0 ]; then
                        failed_sub_array=("${failed_sub_array[@]}" "${error_sub_array[@]}")
                else
                    	failed_sub_array=("${error_sub_array[@]}")
                fi
        fi

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
	
	if [ ${#error_sub_array[@]} -gt 0 ]; then
		local error="True"
		printf -v error_joined '%s,' "${error_sub_array[@]}"
		if [[ "$check" == "True" ]]; then
			echo "The following subjects contain errors: "
			echo "${error_joined%,}"
		fi
	fi

	# Check all subject directories exist
	local raw_sub_array derivatives_sub_array unique_array
	mapfile -t raw_sub_array < <(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' )
	if [[ "$check_corral" == "True" ]]; then
		mapfile -t derivatives_sub_array < <(find "$derivatives_inprocess_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' )
	else
		mapfile -t derivatives_sub_array < <(find "$derivatives_scratch_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' )
	fi
	mapfile -t unique_array < <(comm -3 <(printf "%s\n" "${raw_sub_array[@]}" | sort) <(printf "%s\n" "${derivatives_sub_array[@]-}" | sort) | sort -n) # print unique elements
	
	local incomplete="False"
	if [ ${#unique_array[@]} -gt 0 ]; then
		incomplete="True"
		local unique_joined
		printf -v unique_joined '%s,' "${unique_array[@]}"
		if [[ "$check" == "True" ]]; then
			echo "The following subjects dirs do not exist: "
			echo "${unique_joined%,}"
		fi
	fi
	
	if [ ${#sub_array[@]} -gt 0 ]; then
		incomplete="True"
		printf -v sub_joined '%s,' "${sub_array[@]}"
		if [[ "$check" == "True" ]]; then
			echo "The following subjects have not been run: "
			echo "${sub_joined%,}"
		fi
	fi
	
	total_run_subs=$(bc -l <<< "( ${#success_sub_array[@]} + ${#failed_sub_array[@]} + ${#error_sub_array[@]} )" )
	if [ ${#success_sub_array[@]} -eq 0 ]; then
		success_percent=0
	elif [ ${#failed_sub_array[@]} -eq 0 ] && [ ${#error_sub_array[@]} -eq 0 ]; then
		success_percent=100
	else
		success_percent=$(bc -l <<< "scale = 10; ( ${#success_sub_array[@]} / $total_run_subs ) * 100" )
	fi
	echo "${success_percent:0:4}% (${#success_sub_array[@]}/$total_run_subs) of attempted subjects were successful."
	
	printf '%s\n' "${runtime_array[@]-}"
	
	if [ ${#failed_sub_array[@]} -gt 0 ]; then
		fail_array+=("$raw_ds")
	elif [ ${#error_sub_array[@]} -gt 0 ]; then
		error_array+=("$raw_ds")
	elif [[ "$incomplete" == "False" ]]; then
		success_array+=("$raw_ds")
	else
		incomplete_array+=("$raw_ds")
	fi
	
	success_sub_count=$(($success_sub_count + ${#success_sub_array[@]}))
	error_sub_count=$(($error_sub_count + ${#error_sub_array[@]}))
	
	if [[ "$purge" == "True" ]]; then
		for sub in "${success_sub_array[@]-}"; do
			rm -rf "$work_dir/${raw_ds}_sub-$sub"
		done
	fi
	if [[ "$tar" == "True" ]]; then
		for sub in "${failed_sub_array[@]-}"; do
			echo "$sub"
			if [[ -d "$work_dir/${raw_ds}_sub-$sub" ]]; then
				tar -cvf "$work_dir/${raw_ds}_sub-$sub".tar "$work_dir/${raw_ds}_sub-$sub" 
				rm -rf "$work_dir/${raw_ds}_sub-$sub"
			fi
		done
	fi
}

git_log_check () {
	local raw_ds="$1"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	local derivatives_inprocess_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"
	
	scratch_commit=$(git -C "$derivatives_scratch_path" rev-parse HEAD)
	inprocess_commit=$(git -C "$derivatives_inprocess_path" rev-parse HEAD)
	if [[ $scratch_commit != $inprocess_commit ]]; then
		echo -n "$raw_ds,"
	fi
}

clone_derivatives () {
	local raw_ds="$1"
	local derivatives_scratch_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
	local derivatives_inprocess_path="$OPENNEURO/in_process/$software/${raw_ds}-${software}"
	local derivatives_final_path="$OPENNEURO/$software/${raw_ds}-${software}"
	local raw_path="$STAGING/raw/$raw_ds"
	
	if [ -d "$derivatives_scratch_path" ]; then
		push "$raw_ds"	
	fi
	
	# Move remora logs to corral
	if compgen -G "$derivatives_inprocess_path/remora*"; then
		datalad unlock -d "$derivatives_inprocess_path" "$derivatives_inprocess_path"/remora*
		mkdir -p "$OPENNEURO/logs/$software/remora/${raw_ds}-${software}-remora/"
		mv "$derivatives_inprocess_path"/remora* "$OPENNEURO/logs/$software/remora/${raw_ds}-${software}-remora/"
		datalad save -d "$derivatives_inprocess_path" -m "remove remora logs from ds"
	fi
	
	mv "$derivatives_inprocess_path" "$derivatives_final_path"
	cd "$derivatives_final_path" || exit
	git config --file .gitmodules --replace-all submodule.code/containers.url https://github.com/ReproNim/containers.git
	git config --file .gitmodules --unset-all submodule.code/containers.datalad-url
	git config --file .gitmodules --replace-all submodule.sourcedata/raw.url https://github.com/OpenNeuroDatasets/"$raw_ds".git
	git config --file .gitmodules --unset-all submodule.sourcedata/raw.datalad-url
	git config --file .gitmodules --replace-all submodule.sourcedata/templateflow.url https://github.com/templateflow/templateflow.git
	git config --file .gitmodules --unset-all submodule.sourcedata/templateflow.datalad-url
	git-annex lock
	datalad save -r -m "change gitmodule urls to origin"
	datalad install . -r
	
	if [[ "$software" == "fmriprep" ]]; then
		chmod -R 775 "$raw_path"
		rm -rf "$raw_path"
	fi
	
	if [ -d "$derivatives_scratch_path" ]; then
		chmod -R 775 "$derivatives_scratch_path"
		rm -rf "$derivatives_scratch_path"
	fi
	rm -rf "$SCRATCH/work_dir/$software/$raw_ds"*
}

group () {
	local raw_ds="$1"
	local derivatives_final_path="$OPENNEURO/$software/${raw_ds}-${software}"
	local raw_path="$OPENNEURO/raw/$raw_ds"
	
	if [[ ! -f "$derivatives_final_path/group_T1w.html" ]]; then
		echo "$raw_ds"
		download_raw_ds "$raw_ds"
		cd "$derivatives_final_path" || exit
		mriqc "$raw_path" . group -w "$OPENNEURO"/mriqc/work/"$raw_ds" || exit
		datalad save -m "group report"
	fi

}

publish () {
	local raw_ds="$1"
	local derivatives_final_path="$OPENNEURO/$software/${raw_ds}-${software}"
	local OpenNeuroDerivatives_path="$OPENNEURO/OpenNeuroDerivatives_github/OpenNeuroDerivatives"
	
	cd "$derivatives_final_path"
	datalad siblings -s openneuro-derivatives || git-annex initremote openneuro-derivatives type=S3 bucket=openneuro-derivatives exporttree=yes versioning=yes partsize=1GiB encryption=none \
		fileprefix="${software}"/"${raw_ds}-${software}"/ autoenable=true publicurl=https://openneuro-derivatives.s3.amazonaws.com public=yes
	git annex export main --to openneuro-derivatives
	git annex enableremote openneuro-derivatives publicurl=https://openneuro-derivatives.s3.amazonaws.com public=no
	datalad create-sibling-github -d . OpenNeuroDerivatives/"${raw_ds}-${software}" --publish-depends openneuro-derivatives --access-protocol ssh --existing reconfigure --credential datalad.credential.helper
	datalad push --to github -f checkdatapresent
	gh repo edit OpenNeuroDerivatives/"${raw_ds}-${software}" --description ''
	sleep 5
	datalad clone -d "$OpenNeuroDerivatives_path" https://github.com/OpenNeuroDerivatives/"${raw_ds}-${software}".git "$OpenNeuroDerivatives_path/${raw_ds}-${software}"
}

# initialize variables
user_email="jbwexler@tutanota.com"
software="$1"
STAGING="$SCRATCH/openneuro_derivatives"
OPENNEURO="/corral-repl/utexas/poldracklab/data/OpenNeuro"
work_dir="$SCRATCH/work_dir/$software"
fs_license=$HOME/.freesurfer.txt # this should be in code/license

syn_sdc="True"
skull_strip="force"
subs_per_job="200"
all_subs_arg=""
subs_per_node=""
dataset_list=""
skip_raw_download="False"
skip_create_derivatives="False"
skip_run_software="False"
skip_setup_scratch="False"
download_create_run="True"
skip_raw_clone="False"
clone_derivatives="False"
ignore_check="False"
push="False"
remaining="False"
rerun="False"
check="False"
check_corral="False"
errors="False"
purge="False"
tar="False"
publish="False"
part="1"
group="False"
git_log_check="False"
skip_push="False"
skip_rsync="False"
freesurfer_6="False"
ignore_errors="False"
walltime=""

# initialize flags
while [[ "$#" -gt 0 ]]; do
  case "$1" in
	--no-syn-sdc)
		syn_sdc="False" ;;
	--skull-strip-t1w)
		skull_strip="$2"; shift ;;
	--sub-list)
		all_subs_arg="$2"; shift ;;
	--subs-per-job)
		subs_per_job="$2"; shift ;;
	--subs-per-node)
		subs_per_node="$2"; shift ;;
	--dataset-file)
		dataset_list=$(cat "$2"); shift ;;
	--dataset)
		dataset_list="${2//,/$'\n'}"; shift ;;
	--dataset-all)
		dataset_list=$(find "$STAGING"/derivatives/"$software"/ -maxdepth 1 -name "ds*" | sed -r 's/.*(ds......).*/\1/g') ;;
	--dataset-all-cloned)
		dataset_list=$(find "$OPENNEURO"/"$software"/ -maxdepth 1 -name "ds*" | sed -r 's/.*(ds......).*/\1/g') ;;
	--skip-raw-download)
		skip_raw_download="True" ;;
	--skip-create-derivatives)
		skip_create_derivatives="True" ;;
	--skip-run-software)
		skip_run_software="True" ;;
	--just-run-software)
		skip_push="True"
		skip_raw_download="True"
		skip_create_derivatives="True"
		skip_setup_scratch="True" ;;
	--skip-setup-scratch)
		skip_setup_scratch="True" ;;
	--skip-raw-clone)
		skip_raw_clone="True" ;;
	--clone)
		clone_derivatives="True"
		download_create_run="False" ;;
	--ignore-check)
		ignore_check="True" ;;
	--push)
		download_create_run="False"
		push="True" ;;
	--remaining)
		remaining="True" ;;
	--rerun)
		rerun="True" ;;
	--check)
		check="True"
		clone_derivatives="True"
		download_create_run="False" ;;
	--check-corral)
		check="True"
		clone_derivatives="True"
		download_create_run="False"
		check_corral="True" ;;
	--errors)
		errors="True" ;;
	--purge)
		purge="True" ;;
	--tar)
		tar="True" ;;
	--part)
		part="$2"; shift ;;
	--publish)
		download_create_run="False"
		publish="True" ;;
	--group)
		group="True"
		download_create_run="False" ;;
	--group-all)
		dataset_list=$(find "$OPENNEURO"/mriqc/ -maxdepth 1 -name "ds*" | sed -r 's/.*(ds......).*/\1/g')
		download_create_run="False"
		group="True" ;;
	--git-log-check)
		download_create_run="False"
		git_log_check="True" ;; 
	--skip-rsync)
		skip_rsync="True" ;;
	--freesurfer-6)
		freesurfer_6="True" ;;
	--ignore-errors)
		ignore_errors="True" ;;
	--walltime)
		walltime="$2"; shift ;;
        --just-download-raw)
                skip_create_derivatives="True"
                skip_run_software="True"
                skip_push="True"
                skip_setup_scratch="True" ;; 
	-x)
		set -x ;;
  esac
  shift
done

if [ -z "$dataset_list" ]; then
	echo "No datasets list provided"
	exit 1
fi

if [ -z "$walltime" ]; then
	if [[ "$software" == "fmriprep" ]]; then
                walltime="48:00:00"
	elif [[ "$software" == "mriqc" ]]; then
                walltime="8:00:00"
	fi
fi

# run full pipeline
if [[ "$download_create_run" == "True" ]]; then
	if [[ "$skip_push" == "False" ]]; then
		while IFS= read -r raw_ds; do  
			if [ -d "$STAGING/derivatives/$software/${raw_ds}-${software}" ]; then
				push "$raw_ds"
			fi
		done <<< "$dataset_list"
	fi
	if [[ "$skip_raw_download" == "False" ]]; then
		while IFS= read -r raw_ds; do  
			download_raw_ds "$raw_ds"
		done <<< "$dataset_list"
	fi
	if [[ "$skip_create_derivatives" == "False" ]]; then
		if [[ "$skip_rsync" == "False" ]]; then
			if [[ "$freesurfer_6" == "True" ]]; then
				rsync -av --delete "$OPENNEURO/software/containers_freesurfer6" "$STAGING" --include ".*"
			else
				rsync -av --delete "$OPENNEURO/software/containers" "$STAGING" --include ".*"
			fi
			rsync -av --delete "$OPENNEURO/software/templateflow" "$STAGING" --include ".*"
		fi
		find "$STAGING/containers" -exec touch -h {} +
		find "$STAGING/templateflow" -exec touch -h {} +
		while IFS= read -r raw_ds; do  
			create_derivatives_ds "$raw_ds"
		done <<< "$dataset_list"	
	fi
	if [[ "$skip_setup_scratch" == "False" ]]; then
		while IFS= read -r raw_ds; do  
			setup_scratch_ds "$raw_ds"
		done <<< "$dataset_list"		
	fi			
	if [[ "$skip_run_software" == "False" ]]; then
		while IFS= read -r raw_ds; do  
			run_software "$raw_ds"
		done <<< "$dataset_list"		
	fi	
elif [[ "$clone_derivatives" == "True" ]]; then
	if [[ "$ignore_check" != "True" ]]; then
		while IFS= read -r raw_ds; do  
			check_results "$raw_ds"
		done <<< "$dataset_list"
		printf -v success_print "%s," "${success_array[@]-}"
		printf -v failed_print "%s," "${fail_array[@]-}"
		printf -v error_print "%s," "${error_array[@]-}"
		printf -v incomplete_print "%s," "${incomplete_array[@]-}"
		echo -e "\nSuccess: "
		echo "${success_print%,}"
		echo -e "Failed: "
		echo "${failed_print%,}"
		echo -e "Error: "
		echo "${error_print%,}"
		echo -e "Incomplete: "
		echo "${incomplete_print%,}"
		clone_list="$(echo ${success_print%,} | sed 's/,/\n/g')"
		echo -e "\nSuccess subject count: "
		echo "$success_sub_count"
		echo -e "\nError subject count: "
		echo "$error_sub_count"
	else
		clone_list="$dataset_list"
	fi
	if [[ "$check" != "True" ]]; then
		while IFS= read -r raw_ds; do  
			clone_derivatives "$raw_ds" 
		done <<< "$clone_list"
	fi
elif [[ "$push" == "True" ]]; then
	while IFS= read -r raw_ds; do  
		push "$raw_ds"
	done <<< "$dataset_list"
elif [[ "$publish" == "True" ]]; then
	source /home1/03201/jbwexler/scripts/export_aws_keys.sh
	while IFS= read -r raw_ds; do  
		publish "$raw_ds"
	done <<< "$dataset_list"
	datalad push -d $OPENNEURO/OpenNeuroDerivatives_github/OpenNeuroDerivatives --to github
elif [[ "$group" == "True" ]]; then
	# need to manually run 'conda activate mriqc'
	while IFS= read -r raw_ds; do  
		group "$raw_ds"
	done <<< "$dataset_list"
elif [[ "$git_log_check" == "True" ]]; then
	echo "The following datasets have not been successfully pushed:"
	while IFS= read -r raw_ds; do  
		git_log_check "$raw_ds"
	done <<< "$dataset_list"
	echo
fi



