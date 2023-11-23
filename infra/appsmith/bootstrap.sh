#!/bin/bash

curl -L https://bit.ly/docker-compose-be -o ./docker-compose.yml
docker-compose up -d

# connect appsmith with pgvector_default network
docker network connect pgvector_default appsmith

pgvector_ip=$(docker inspect --format='{{.NetworkSettings.Networks.pgvector_default.IPAddress}}' pgvector)

echo "Found PG vector ip: $pgvector_ip"



