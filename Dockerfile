# Use an x86 base image
# Stage 1: Base Image with Miniconda
FROM continuumio/miniconda3:24.5.0-0 as core

# Set the working directory
WORKDIR /app

ADD . /app/repo

#ARG GITHUB_REPO_URLls -l
#RUN git clone --depth 1 ${GITHUB_REPO_URL} repo

# Move to the repository directory
WORKDIR /app/repo

# Force all commands to run in bash
SHELL ["/bin/bash", "--login", "-c"]

# install the conda build package in base
RUN conda install -y conda-build

# Initialize the conda environment 
RUN conda env create -f environment-prod.yaml

# Initialize conda in bash config files:
RUN conda init bash

# Activate the environment, and make sure it's activated:
RUN echo "conda activate alethic-ism-core" > ~/.bashrc

# display information about the current activation
RUN conda info

# Install necessary dependencies for the build process
RUN conda update -n base -c defaults conda && \
    conda install -y conda-build && \
    #conda install -c conda-forge conda-libmamba-solver && \
    conda config --set solver classic && \
    conda list | grep solver

#RUN conda install --solver=classic conda-forge::conda-libmamba-solver conda-forge::libmamba conda-forge::libmambapy conda-forge::libarchive

# Run the build command (adjust as per your repo's requirements)
#RUN conda build . --output-folder /app/local-channel
RUN bash ./conda_build.sh --local-channel-path /app/conda/env/local_channel

# package the local channel such that we can extract into an artifact

RUN chmod +x ./package-conda-channel.sh
RUN bash ./package-conda-channel.sh
