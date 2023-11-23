#!/bin/bash


docker run \
    --name pgvector \
    -p 5432:5432 \
    -e POSTGRES_USER=postgres \
    -e POSTGRES_PASSWORD=postgres1 \
    -e POSTGRES_DB=postgres \
    -d \
    ankane/pgvector