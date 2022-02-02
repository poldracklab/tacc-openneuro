#!/bin/bash

# Clone/update raw datasets and download necessary data for fmriprep/mriqc
download_raw_ds () {
	while IFS= read -r raw_ds; do  
		raw_path="$STAGING/raw/$raw_ds"
		derivatives_path="$STAGING/derivatives/$software/${raw_ds}-${software}"
		
		if [[ -d "$raw_path" ]] && [[ ! -f "$raw_path/dataset_description.json" ]] || [[ "$(git -C "$raw_path" fsck)" == *"dangling"* ]]; then
			# Delete datasets on $SCRATCH that have been purged by TACC
			chmod -R 775 "$raw_path"
			rm -rf "$raw_path"
			datalad remove "$derivatives_path/sourcedata/raw" --nocheck -r
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

		# to-do: fix reckless clone issue when original dataset doesn't download properly 
		if [[ ! -d "$derivatives_path" ]]; then
			datalad create -c yoda "$derivatives_path"
			cd "$derivatives_path"
			rm CHANGELOG.md README.md code/README.md
			datalad clone -d . "$STAGING/containers" code/containers --reckless ephemeral
			git clone https://github.com/poldracklab/tacc-openneuro.git code/tacc-openneuro
			mkdir sourcedata
			datalad clone -d . "$raw_path" sourcedata/raw --reckless ephemeral
			datalad clone -d . "$STAGING/templateflow" sourcedata/templateflow --reckless ephemeral
	  
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
			if [[ ! -d "$derivatives_path/sourcedata/raw" ]]; then
				cd "$derivatives_path"
				datalad clone -d . "$raw_path" sourcedata/raw --reckless ephemeral
			else
				datalad update --merge -d "$derivatives_path/sourcedata/raw"
			fi
			datalad update --merge -d "$derivatives_path/code/containers"
			datalad update --merge -d "$derivatives_path/code/tacc-openneuro"	
			datalad update --merge -d "$derivatives_path/sourcedata/templateflow"	  
			  
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
						"--skull-strip-t1w" "$skull_strip" "--mem_mb" "$mem_mb" "--bids-database-dir" "/tmp")
			if [[ "$syn_sdc" ==  "True" ]]; then
				command+=("--use-syn-sdc")
				command+=("warn")
			fi
			
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

		export SINGULARITYENV_TEMPLATEFLOW_HOME="sourcedata/templateflow/"
		export SINGULARITYENV_TEMPLATEFLOW_USE_DATALAD="true"
		# Submit jobs via reproman in batches 
		# make sure to 'unlock' outputs
		count=0
		echo "$all_subs" | xargs -n "$subs_per_job" echo | while read line; do 
			((count++))

			if [ "$part" != "$count" ]; then
				continue
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
		raw_path="$STAGING/raw/$raw_ds"
		
		# Check results
		fail="False"
		if [[ "$software" == "mriqc" ]]; then
			success_phrase="MRIQC completed"
		elif [[ "$software" == "fmriprep" ]]; then
			success_phrase="fMRIPrep finished successfully"
		fi
		reproman_logs="$(ls -1d $derivatives_path/.reproman/jobs/local/* | sort -nr)"
		readarray -t sub_array < <(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' | sort )
		failed_array=()
		while IFS= read -r job_dir && [ ${#sub_array[@]} -gt 0 ]; do		
			echo "$job_dir"
			for stdout in "$job_dir"/stdout.*; do
				sub=$(head -n 10 "$stdout" | grep "\-\-participant-label" | sed -r 's/.*--participant-label \x27([^\x27]*)\x27.*/\1/g'01)
				# Look for exact match in array
				if [[ ${sub_array[*]} =~ (^|[[:space:]])"$sub"($|[[:space:]]) ]]; then
					# Remove sub from array
					for i in "${!sub_array[@]}";do
						if [[ "${sub_array[$i]}" == "$sub" ]];then 
							unset 'sub_array[i]'
							break
						fi
					done
					if [[ "$(tail -n 10 $stdout)" != *"$success_phrase"* ]]; then
						echo "$stdout failed"
						failed_array+=("$sub")
					fi
				fi
			done
		done <<< "$reproman_logs"
		
		if [ ${#failed_array[@]} -gt 0 ]; then
			fail="True"
			echo "The following subjects failed: "
			printf -v failed_joined '%s,' "${failed_array[@]}"
			echo "${failed_joined%,}"
		fi
		
		# Check all subject directories exist
		readarray -t raw_sub_array < <(find "$raw_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' )
		readarray -t derivatives_sub_array < <(find "$derivatives_path" -maxdepth 1 -type d -name "sub-*" -printf '%f\n' | sed 's/sub-//g' )
		unique_array=($(comm -3 <(printf "%s\n" "${raw_sub_array[@]}" | sort) <(printf "%s\n" "${derivatives_sub_array[@]}" | sort) | sort -n)) # print unique elements
		if [ ${#unique_array[@]} -gt 0 ]; then
			fail="True"
			echo "The following subjects dirs do not exist: "
			printf -v unique_joined '%s,' "${unique_array[@]}"
			echo "${unique_joined%,}"
		fi
		
		if [ ${#sub_array[@]} -gt 0 ]; then
			fail="True"
			echo "The following subjects have not been run: "
			printf -v sub_joined '%s,' "${sub_array[@]}"
			echo "${sub_joined%,}"
		fi
		
		if [[ "$fail" == "True" ]]; then
			success_array+=("${raw_ds}: fail")
			if [[ "$ignore" != "True" ]]; then
				continue
			fi
		else	
			success_array+=("${raw_ds}: success")
		fi		
		
		
		if [[ "$check" == "True" ]]; then
			continue
		fi
		
		# Move remora logs to corral
		datalad unlock -d "$derivatives_path" "$derivatives_path"/remora*
		mkdir "$OPENNEURO/logs/$software/remora/${raw_ds}-${software}-remora/"
		mv "$derivatives_path"/remora* "$OPENNEURO/logs/$software/remora/${raw_ds}-${software}-remora/"
		datalad save -d "$derivatives_path"
		
		datalad clone "$derivatives_path" "$derivatives_path_corral"
		cd "$derivatives_path_corral/$ds"
		datalad get .
		git config --file .gitmodules --replace-all submodule.code/containers.url https://github.com/ReproNim/containers.git
		git config --file .gitmodules --unset-all submodule.code/containers.datalad-url
		git config --file .gitmodules --replace-all submodule.sourcedata/raw.url https://github.com/OpenNeuroDatasets/"$raw_ds".git
		git config --file .gitmodules --unset-all submodule.sourcedata/raw.datalad-url
		git config --file .gitmodules --unset-all submodule.sourcedata/templateflow.datalad-url
		datalad save -r
		datalad install . -r
		
		derivatives_path_old="$STAGING/derivatives/$software/old/${raw_ds}-${software}"
		if [[ -d "$derivatives_path_old" ]]; then
			chmod -R 775 "$derivatives_path_old"
			rm -rf "$derivatives_path_old"
		fi
		mv -f "$derivatives_path" "$derivatives_path_old"
	done <<< "$dataset_list"
	echo
	printf "%s\n" "${success_array[@]}"
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
		ss_force=$2; shift ;;
	--sub-list)
		all_subs_arg=$2; shift ;;
	--subs-per-job)
		subs_per_job=$2; shift ;;
	--subs-per-node)
		subs_per_node=$2; shift ;;
	--dataset-file)
		dataset_list=$(cat $2); shift ;;
	--dataset)
		dataset_list=$(echo $2 | sed 's/,/\n/gi'); shift ;;
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
		download_raw_ds
	fi
	if [[ "$skip_create_derivatives" == "False" ]]; then
		datalad update --merge -d "$STAGING/templateflow"
		create_derivatives_ds
	fi
	if [[ "$skip_run_software" == "False" ]]; then
		run_software
	fi
elif [[ "$clone_derivatives" == "True" ]]; then
	clone_derivatives
fi
	
