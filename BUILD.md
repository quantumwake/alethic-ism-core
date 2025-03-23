# Alethic Instruction State Machine (Core Python SDK)

### Requirements
`pip install uv`

### Environment
`uv venv`

`source .venv/bin/activate`

### Build package 
`uv pip install build`

`uv pip install -U pip twine setuptools build`

`uv pip install -e .`

`python -m build`


### Docker Build & Upload PyPi package
`sh docker_build.sh -t krasaee/alethic-ism-core:latest -a linux/amd64`