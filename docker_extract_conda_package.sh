#!/bin/bash

container_id=$(docker create quantumwake/alethic-ism-core:latest)
echo "Container ID: $container_id" # For debugging

docker images # Optional: For debugging, to list available images
file_name=$(docker run --rm --entrypoint find "quantumwake/alethic-ism-core:latest" /app/local-channel/linux-64 -name 'alethic-ism-core-*-*_*.tar.bz2' -exec basename {} \;)
echo "File name: $file_name" # For debugging

docker cp "$container_id:/app/local-channel/linux-64/$file_name" .
echo "::set-output name=file_name::$file_name"

