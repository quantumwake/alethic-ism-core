# Use an x86 base image
# Stage 1: Base Image with Miniconda
FROM continuumio/miniconda3 as base

# Set the working directory
WORKDIR /app

ARG GITHUB_REPO_URL
RUN git clone --depth 1 ${GITHUB_REPO_URL} repo

# Move to the repository directory
WORKDIR /app/repo

# Force all commands to run in bash
SHELL ["/bin/bash", "--login", "-c"]

# Initialize the conda environment 
RUN conda env create -f environment.yml

# Initialize conda in bash config files:
RUN conda init bash

# Activate the environment, and make sure it's activated:
RUN echo "conda activate alethic-ism-core" > ~/.bashrc

# Install necessary dependencies for the build process
RUN conda install -y conda-build

# Run the build command (adjust as per your repo's requirements)
#RUN conda build . --output-folder /app/local-channel
RUN bash ./build.sh

# Install the anaconda client to upload
#RUN conda install anaconda-client

# (Optional) Command to keep the container running, adjust as needed
#CMD tail -f /dev/null


