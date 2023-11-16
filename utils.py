import json
import os

# only keep alphanumerical values and spaces, where spaces is converted to an underscore '_'
clean_char_for_ddl_naming = lambda x: (x if x.isalnum() or x == '_' else ' ' if x == '.' or x.isspace() else '')
clean_string_for_ddl_naming = lambda s: "_".join(''.join([clean_char_for_ddl_naming(c) for c in s]).split(' '))


def merge_nested_dicts(d1, d2):
    for key, value in d2.items():
        if key in d1 and isinstance(d1[key], dict) and isinstance(value, dict):
            merge_nested_dicts(d1[key], value)
        else:
            d1[key] = value

    return d1


def build_column_name(name: str):
    return clean_string_for_ddl_naming(name).lower()


def build_table_name(header: dict):
    unique_name = header['name'] if 'name' in header else None

    def prefix(name, from_header: dict = header):
        _prefix = from_header[name].strip() if name in from_header else None

        if _prefix:
            return clean_string_for_ddl_naming(_prefix).lower()

        return str()

    if not unique_name:
        provider = prefix('provider_name')
        model_name = prefix('model_name')
        user_template = prefix('name', from_header=header['user_template'])
        system_template = prefix('name', from_header=header['system_template'])

        table_name_appender_list = f"STATE_{provider} {model_name} {user_template} {system_template}".split()
        unique_name = "_".join([x for x in table_name_appender_list if x])

    return clean_string_for_ddl_naming(unique_name).lower()


def convert_string_to_instanceof(value):
    if not value:
        return None

    # Convert to boolean

    if not isinstance(value, str):
        return value

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
