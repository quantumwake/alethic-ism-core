import json
import os

def convert_string_to_instanceof(value):
    if not value:
        return None

    # Convert to boolean
    if value.lower() in ["true", "false"]:
        return value.lower() == "true"

    # Convert to integer or float
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            # Return as string if all else fails
            return value

def load_state(state_file: str):
    state = {}
    if not os.path.exists(state_file):
        raise FileNotFoundError(
            f'state file {state_file} not found, please use the ensure to use the state template to create the input state file.')

    with open(state_file, 'r') as fio:
        state = json.load(fio)

        if 'header' not in state:
            raise Exception(f'Invalid state input, please make sure you define '
                            f'a basic header for the input state')

        if 'data' not in state:
            raise Exception(f'Invalid state input, please make sure a data field exists '
                            f'in the root of the input state')

        data = state['data']

        if not isinstance(data, list):
            raise Exception('Invalid data input states, the data input must be a list of dictionaries '
                            'List[dict] where each dictionary is FLAT record of key/value pairs')

    return state