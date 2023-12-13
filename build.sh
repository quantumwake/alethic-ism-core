#!/bin/bash

i=$(cat ./conda-recipe/meta.yaml | awk '/number: [0-9]+/ {printf("%s+1\n", $2)}' | bc)
cat ./conda-recipe/meta.yaml | sed -E 's/number: [0-9]+/number: '''$i'''/g' > tmp_meta.yaml
mv ./conda-recipe/meta.yaml /tmp/meta_backup.yaml
mv tmp_meta.yaml ./conda-recipe/meta.yaml

cat meta.yaml | grep "number: "

#conda install -c defaults conda-build

META_YAML_PATH="./conda-recipe/meta.yaml"

#python ./increment_build_no.py
#rm -rf /condalocal-channel/*
#conda build purge

yes | conda clean --all
conda build ./conda-recipe --output-folder ~/miniconda3/envs/local_channel
conda index ~/miniconda3/envs/local_channel
