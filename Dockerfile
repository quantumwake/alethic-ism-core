FROM ghcr.io/astral-sh/uv:python3.12-alpine as core
# conda package repository token such that we can upload the package to anaconda cloud
#ARG PYPI_API_KEY
#ENV PYPI_API_KEY=$PYPI_API_KEY

# Set the working directory
WORKDIR /app

ADD . .

# Create a virtual environment using uv
RUN uv venv

# Install your package in editable mode using the virtual environment’s pip
RUN source .venv/bin/activate && \
    apk update && \
    apk add git && \
    uv pip install -U pip twine setuptools setuptools-scm build && \
    uv pip install -r requirements.txt

# Build the package using the virtual environment’s Python interpreter
RUN source .venv/bin/activate && \
    python -m build

RUN --mount=type=secret,id=pypirc,target=/root/.pypirc source .venv/bin/activate && \
    python -m twine upload --repository pypi dist/*

