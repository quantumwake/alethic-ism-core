# Dependencies

Use conda build via the build.sh script

- python-dotenv,
- pyyaml,
- pydantic

# Initialize Conda

- conda init
- conda create -n local_channel --no-default-packages
- conda create -n alethic-ism-core python=3.11
- conda install conda-build
- bash build.sh

# Upload package to Anaconda
- conda install anaconda-client conda-build


# Testing
- conda install pytest

