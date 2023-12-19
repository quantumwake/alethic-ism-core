#!/bin/bash

image="quantumwake/alethic-ism-core:latest"
search_path=/app/conda/env/local_channel
search_pattern="alethic-ism-core-*-*_*.tar.bz2"

container_id=$(docker create quantumwake/alethic-ism-core:latest)
echo "Container ID: $container_id" # For debugging

docker images # Optional: For debugging, to list available images

## 
version_file=$(docker run --rm --entrypoint find "$image" "$search_path" -name "$search_pattern")
version_file=$(basename $version_file)

# zip the entire channel

docker run --rm --entrypoint /app/entrypoint-extract.sh "$image"

# extract the gzip package
echo "File name: $file_name located in docker image at $file_path" # For debugging
docker cp "$container_id:/tmp/$version_file" $version_file

echo "checking env file_name: $file_name"
echo "::set-output name=file_name::$file_name"

