#!/bin/bash

git clone https://github.com/apache/superset.git
cd superset

docker compose -f docker-compose-non-dev.yml pull
docker compose -f docker-compose-non-dev.yml up

docker network connect pgvector_default superset_app
docker network connect pgvector_default superset_worker
docker network connect pgvector_default superset_worker_beat

