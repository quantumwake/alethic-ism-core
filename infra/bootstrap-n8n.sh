#!/bin/bash
#
docker volume create n8n_data

#docker run -it --rm --name n8n -p 5678:5678 -v n8n_data:/home/node/.n8n docker.n8n.io/n8nio/n8n

PGVECTOR_IP=$(docker inspect --format='{{.NetworkSettings.Networks.pgvector_default.IPAddress}}' pgvector)

docker run -it --rm \
 --name n8n \
 -p 5678:5678 \
 -e DB_TYPE=postgresdb \
 -e DB_POSTGRESDB_DATABASE=postgres \
 -e DB_POSTGRESDB_HOST=$PGVECTOR_IP \
 -e DB_POSTGRESDB_PORT=5432 \
 -e DB_POSTGRESDB_USER=postgres \
 -e DB_POSTGRESDB_SCHEMA=public \
 -e DB_POSTGRESDB_PASSWORD=postgres1 \
 -v n8n_data:/home/node/.n8n \
docker.n8n.io/n8nio/n8n


