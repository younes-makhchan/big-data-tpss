#!/bin/bash

# Function to recursively rename directories
rename_directories_recursively() {
    local current_dir="$1"

    # Change to the current directory
    cd "$current_dir" || return

    # Loop through all directories in the current directory
    for dir in */; do
        # Remove the trailing slash from the directory name
        dir_name="${dir%/}"

        # Check if the directory name matches "errami"
        if [ "$dir_name" == "anas" ]; then
            echo "Directory 'anas' found in $PWD. Renaming to 'makhchan'."
            # Rename the directory
            mv "$dir_name" "younes"
            echo "Directory renamed successfully."
        fi

        # If it is a directory, recurse into it
        if [ -d "$dir_name" ]; then
            rename_directories_recursively "$dir_name"
            # Return to the parent directory
            cd .. || return
        fi
    done
}

# Start the recursive renaming from the current directory
rename_directories_recursively "$PWD"
