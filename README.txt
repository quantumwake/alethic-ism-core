# Requirements

- Conda or equivalent

# Initialize your conda environment
conda create --env alethic-processor
conda activate alethic-processor

conda install pytorch torchvision torchaudio -c pytorch-nightly
conda install huggingface nltk pyter3
conda install psycopg2
conda install python-dotenv




# Initial Python with virtual

## this might not be correct, but it is something similar to this.

e.g. on mac you would do something like

brew install python3
brew install pip3
python3 -m pip install --upgrade pip
python3 -m virtualenv venv
source ./venv/bin/activate

pip install -r requirements.txt

## dotenv ./.env file

Create a file called .env under the root of the project folder

Following envs are required or assign them via the system env

## ask for a key, Kas can provide it
OPENAI_API_KEY=<< api key retrieved from openai >>

# Execute

modify the list of ./generators/philosophers.json for your preference.

python ./generators/philosophers.json

If you get errors, you are probably in the wrong directory or the packages are not installed properly

