#!/bin/bash

APP_NAME="alethic-ism-core"
DOCKER_NAMESPACE="krasaee"
GIT_COMMIT_ID=$(git rev-parse HEAD)
TAG="$DOCKER_NAMESPACE/$APP_NAME:$GIT_COMMIT_ID"

ARCH=$1
if [ -z "$ARCH" ];
then
  ARCH="linux/amd64"
  #TODO check operating system ARCH="linux/arm64"
fi;

docker build --progress=plain \
  --platform $ARCH -t $TAG \
  --build-arg \
  --no-cache .


