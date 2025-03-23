FROM ghcr.io/astral-sh/uv:python3.12-alpine as core
# conda package repository token such that we can upload the package to anaconda cloud
#ARG PYPI_API_KEY
#ENV PYPI_API_KEY=$PYPI_API_KEY

# Set the working directory
WORKDIR /app

ADD . .
#ADD LICENSE /app/repo/LICENSE
#ADD OSS-LICENSE /app/repo/OSS-LICENSE

#ADD src/ismcore /app/repo/src/ismcore
#ADD Makefile /app/repo/Makefile
#ADD requirements.txt /app/repo/requirements.txt
#ADD setup.py /app/repo/setup.py
#ADD pyproject.toml /app/repo/pyproject.toml

# Create a virtual environment using uv
RUN uv venv

# Source the virtual environment
#RUN source .venv/bin/activate

# Install your package in editable mode using the virtual environment’s pip
RUN source .venv/bin/activate && \
    apk update && \
    apk add git && \
    uv pip install -U pip twine setuptools setuptools-scm build && \
    uv pip install -r requirements.txt

# add git information for setuptools-scm to work correctly
#ADD .git /app/repo/.git
#ADD .gitignore /app/repo/.gitignore
#ADD tests
#    uv pip install -e .

# Build the package using the virtual environment’s Python interpreter
RUN source .venv/bin/activate && \
    python -m build
#    python -m twine upload --repository pypi dist/*

