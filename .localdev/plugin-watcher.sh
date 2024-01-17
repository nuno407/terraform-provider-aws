#!/bin/bash

# Define the path to the local directory you want to monitor
local_directory="../../voxel51_plugins/operators-plugins"

# Define the target pod and container path
get_pod_name(){
    kubectl get pod -o custom-columns=:metadata.name --context vcluster-dev --namespace data-ingestion --no-headers -l app=voxel --sort-by=metadata.creationTimestamp
}

container_path="/app/fiftyone/plugins/operators-plugins"
pod_name=""

# Function to copy files to the pod
copy_files_to_pod() {

    # Wait until a "voxel" pod is available and get its name
    while true; do
        echo "getting pod name"
        pod_name=$(get_pod_name)
        if [ -n "$pod_name" ]; then
            echo "pod_name is $pod_name"
            break
        fi
        sleep 15
    done
    kubectl cp --context vcluster-dev --namespace data-ingestion "$local_directory" "$pod_name:$container_path"
}

copy_files_to_pod

# Function to calculate the checksum of a file
calculate_checksum() {
    file="$1"
    checksum=$(md5sum "$file" | awk '{print $1}')
    echo "$checksum"
}

# Create a dictionary to store the checksums of files
declare -A file_checksums

# Function to recursively initialize the dictionary with current checksums
initialize_checksums() {
    local current_dir="$1"
    for file in "$current_dir"/*; do
        if [[ -f "$file" ]]; then

            file_checksums["$file"]=$(calculate_checksum "$file")
        elif [[ -d "$file" ]]; then
            initialize_checksums "$file" # Recursively initialize subdirectories
        fi
    done
}

# Initialize the dictionary with the current checksums (including subdirectories)
initialize_checksums "$local_directory"

# Infinite loop to continuously check for changes
while true; do
    echo "Watching for changes in the last 30 seconds"
    # Use find to recursively list all files (including those in subdirectories)
    while IFS= read -r file; do
        current_checksum=$(calculate_checksum "$file")
        previous_checksum="${file_checksums["$file"]}"
        # Compare the current and previous checksums
        if [[ $current_checksum != $previous_checksum ]]; then
            echo "File content changed: $file"
            copy_files_to_pod
            echo "Operator plugins updated"
            # Update the checksum in the dictionary
            file_checksums["$file"]=$current_checksum

        fi
    done < <(find "$local_directory" -type f)
    # Sleep for a while before checking again (adjust the sleep duration as needed)
    sleep 30
done
