# if you are building from scratch and are using a local channel
conda init

# initialize the conda project
conda create -n local_channel --no-default-packages
conda create -n alethic-processor python=3.10
conda install conda-build
pip install -r requirements.txt

# Known Issues

psychopg2 installation
- brew install postgresql
- brew install openssl
- brew link openssl

- export LDFLAGS="-L/opt/homebrew/opt/openssl@1.1/lib"
- export CPPFLAGS="-I/opt/homebrew/opt/openssl@1.1/include"


