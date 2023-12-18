# Use an x86 base image
# Stage 1: Base Image with Miniconda
FROM --platform=linux/amd64 continuumio/miniconda3 as base

# Set the working directory
WORKDIR /app

ARG GITHUB_REPO_URL
RUN git clone --depth 1 ${GITHUB_REPO_URL} repo

# Move to the repository directory
WORKDIR /app/repo

RUN conda env create -f environment.yml

# Activate the environment, and make sure it's activated:
RUN conda activate alethic-ism-core

# Install necessary dependencies for the build process
RUN conda install -y conda-build

# Run the build command (adjust as per your repo's requirements)
RUN conda build . --output-folder /app/local-channel

# (Optional) Command to keep the container running, adjust as needed
#CMD tail -f /dev/null


