# Alethic Instruction-Based State Machine (ISM) Core

This module forms the base layer of the [Alethic ISM project group](https://github.com/quantumwake/alethic), handling the core processor and state management code. It is primarily used for building specific processor types (including language-based processors) and managing the bulk of state input and output processing.

## Key Concepts
- **State Information:** Manages individual state data for specific processing configurations.
- **State Management:** Ensures coherence in managing state column and row data.

## Dependencies
- This project now leverages a modern `pyproject.toml` build system with dynamic versioning powered by `setuptools-scm`.
- Essential libraries include: `python-dotenv`, `pyyaml`, and `pydantic`.

## Environment Setup
1. **Install uv:**  
   Ensure you have the `uv` tool installed:
```bash
  pip install uv
```

## Create & Activate a Virtual Environment:
### Use uv to create and activate your virtual environment:
```bash
    uv venv
    source .venv/bin/activate
```
   
