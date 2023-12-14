# if you are building from scratch and are using a local channel
- conda init

# Initialize Conda

- conda create -n local_channel --no-default-packages
- conda create -n alethic-ism-core python=3.11
- conda install conda-build
- bash build.sh

