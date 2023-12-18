#!/bin/bash

image="quantumwake/alethic-ism-core:latest"
search_path=/app/conda/env/local_channel
search_pattern="alethic-ism-core-*-*_*.tar.bz2"

container_id=$(docker create quantumwake/alethic-ism-core:latest)
echo "Container ID: $container_id" # For debugging

docker images # Optional: For debugging, to list available images
file_path=$(docker run --rm --entrypoint find "$image" "$search_path" -name "$search_pattern")
file_name=$(basename $file_path)
echo "File name: $file_name located in docker image at $file_path" # For debugging
docker cp "$container_id:$file_path" $file_name

echo "checking env file_name: $file_name"
echo "::set-output name=file_name::$file_name"

#export file_name


