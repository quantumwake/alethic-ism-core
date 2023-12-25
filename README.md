# Alethic Instruction-Based State Machine (ISM) Core

This module forms the base layer of the Alethic ISM project, handling the core processor and state management code. It's primarily used for building specific processor types, including language-based processors, and manages the bulk of state input and output processing.

## Key Concepts
- **State Information**: Manages individual state data for specific processing configurations.
- **State Management**: Ensures coherence in managing state column and row data.

## Dependencies
- Utilize `conda build` via `build.sh` script.
- Essential libraries: `python-dotenv`, `pyyaml`, `pydantic`.

## Initialize Conda
- Initiate Conda: `conda init`.
- Create environments: `conda create -n local_channel --no-default-packages` and `conda create -n alethic-ism-core python=3.11`.
- Install Conda Build: `conda install conda-build`.
- Build environment: `bash build.sh`.

## Upload Package to Anaconda
- Install Anaconda Client: `conda install anaconda-client conda-build`.

## Testing
- Install pytest: `conda install pytest`.

## Contribution
Contributions, questions, and feedback are highly encouraged. Contact us for any queries or suggestions.

## License
Released under GNU3 license.

## Acknowledgements
Special thanks to Alethic Research, Princeton University Center for Human Values, and New York University.

---

For more updates and involvement opportunities, visit the [Alethic ISM GitHub page](https://github.com/quantumwake/alethic) or create an issue/comment ticket.
