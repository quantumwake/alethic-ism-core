#!/bin/bash

docker build --platform linux/amd64 -t quantumwake/alethic-ism-core --build-arg GITHUB_REPO_URL=https://github.com/quantumwake/alethic-ism-core.git .

#docker run -d --name alethic-ism-core-build quantumwake/alethic-ism-core

