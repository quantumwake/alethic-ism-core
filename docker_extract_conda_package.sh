#!/bin/bash

image="quantumwake/alethic-ism-core:latest"
container_id=$(docker create quantumwake/alethic-ism-db:latest)
echo "Container ID: $container_id from image $image" # For debugging
docker images # Optional: For debugging, to list available images

# extract the gzip package
file_name="local_channel.tar.gz"
echo "File name: $file_name located in docker image at $file_path" # For debugging
docker cp "$container_id:/app/$file_name" $file_name
echo "::set-output name=file_name::$file_name"
