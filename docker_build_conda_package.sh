#!/bin/bash

#docker build --platform linux/amd64 -t quantumwake/alethic-ism-core:latest \
# --build-arg GITHUB_REPO_URL=https://github.com/quantumwake/alethic-ism-core.git --no-cache .

docker build --progress=plain -t quantumwake/alethic-ism-core:latest \
 --build-arg GITHUB_REPO_URL=https://github.com/quantumwake/alethic-ism-core.git --no-cache .


