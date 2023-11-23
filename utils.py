import hashlib
import json
import logging as log
import os
import re
from typing import Any, List

import map_flattener
from processor.processor_state import StateDataKeyDefinition, StateConfig, StateConfigLM, State

logging = log.getLogger(__name__)

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


def implicit_count_with_force_count(state: State):
    if not state:
        raise Exception(f'invalid state input, cannot be empty or undefined')

    if not isinstance(state, State):
        raise Exception(f'invalid state type, expected {type(State)}, got {type(state)}')

    count = state.count
    logging.info(f'current state data count: {count} for state config {state.config}')

    if count == 0:
        # force derive the count to see if there are rows
        first_column = list(state.columns.keys())[0]
        count = len(state.data[first_column].values)
        state.count = count
        logging.info(f'force update count of state values: {count}, '
                     f'no count found but data exists,for  {state.config}')

    return count


def build_column_name(name: str):
    return clean_string_for_ddl_naming(name).lower()


def build_table_name(config: StateConfig):
    unique_name = config.name if config.name else None

    def prefix(name):
        _prefix = config.name.strip() if name else None

        if _prefix:
            return clean_string_for_ddl_naming(_prefix).lower()

        return str()

    if not unique_name and isinstance(config, StateConfigLM):
        provider = prefix(config.provider_name)
        model_name = prefix(config.model_name)
        user_template = prefix(config.user_template_path)
        system_template = prefix(config.system_template_path)

        table_name_appender_list = f"STATE_{provider} {model_name} {user_template} {system_template}".split()
        unique_name = "_".join([x for x in table_name_appender_list if x])

    return clean_string_for_ddl_naming(unique_name).lower()


def identify_and_return_value_by_type(value):
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


# This function is designed to augment the input dataset with additional data. It offers two methods:
# First, you can add static column/value pairs to each row.
# Second, you can use a function to generate column/value pairs dynamically.
#
# Example Applications:
#   1. Create an embedding from a column in the input dataset for a specific row.
#   2. Add a fixed value to every row, like a header for input state, such as:
#       - provider_name
#       - model_name
#       - etc.
def get_column_state_value(value: Any, *args, **kwargs):
    if not value:
        return None

    if isinstance(value, str):
        return value

    elif callable(value):
        value = value(*args, **kwargs)
        return value

    return value


def has_extension(filename: str, extensions: [str]):
    filename = filename.lower()
    return any(filename.endswith(ext) for ext in extensions)


def load_template(template_file: str):
    if not template_file or not os.path.exists(template_file):
        return None

    with open(template_file, 'r') as fio:
        template_dict = json.load(fio)

    # if template content exists then set it, it can be overwritten by a file
    if 'template_content' in template_dict:
        template_content = template_dict['template_content']
    else:
        template_content = None

    # if a template file exists then try and load it
    if 'template_content_file' in template_dict and template_dict['template_content_file']:

        # throw an exception if both the template_file and template keys are set
        if template_content:
            raise Exception(f'Cannot define a template_content and a template_content_file in the same '
                            f'template configuration {template_file}. For example, if you define the key '
                            f'"template_content" with some text content, you cannot also define a '
                            f'"template_content_file" which points to a different content from a file"')

        # otherwise load the template
        template_file = template_dict['template_content_file']
        with open(template_file, 'r') as fio_tc:
            template_content = fio_tc.read().strip()
            template_dict['template_content'] = template_content

    if not template_content:
        raise Exception(f'no template defined in template file {template_file} with configuration {template_dict}')

    return template_dict


def extract_values_from_query_state_by_key_definition(query_state: dict,
                                                      key_definitions: List[StateDataKeyDefinition] = None):
    # if the key config map does not exist then attempt
    # to use the 'query' key as the key value mapping
    if not key_definitions:
        if "query" not in query_state:
            raise Exception(f'query does not exist in query state {query_state}')

        return query_state['query']

    # iterate each parameter name and look it up in the state object
    results = {}
    for key in key_definitions:
        key_name = key.name
        alias = key.alias

        # if it does not exist, throw an exception to warn the user that the state input is incorrect.
        if key_name not in query_state:
            raise Exception(f'Invalid state input for parameter: {key_name}, '
                            f'query_state: {query_state}, '
                            f'key definition: {key}')

        if not alias:
            alias = key.name

        # add a new key state value pair,
        # constructing a key defined by values from the query_state
        # given that the key exists and is defined in the config_name mapping
        results[alias] = query_state[key_name]

    return results


def calculate_hash(input_string: str):
    # Create a SHA-256 hash object
    hash_object = hashlib.sha256()

    # Update the hash object with the bytes of the string
    hash_object.update(input_string.encode())

    # Get the hexadecimal representation of the hash
    return hash_object.hexdigest()


def calculate_string_list_hash(names: [str]):
    plain = ",".join([f'({idx}:{key})' for idx, key in enumerate(sorted(names))])
    return calculate_hash(plain)


def calculate_string_dict_hash(item: dict):
    plain = ",".join([f'({key}:{value})' for key, value in item.items()])
    return calculate_hash(plain)


def build_state_data_row_key(query_state: dict, key_definitions: List[StateDataKeyDefinition]):
    try:
        # this will be used as the primary key
        state_key_values = extract_values_from_query_state_by_key_definition(
            key_definitions=key_definitions,
            query_state=query_state)

        # iterate each primary key value pair and create a tuple for hashing
        keys = [(name, value) for name, value in state_key_values.items()]

        # hash the keys as a string in sha256
        return calculate_hash(str(keys)), keys
    except Exception as e:
        error = (f'error in trying to build state data row key for key definition {key_definitions} '
                 f'and query_state {query_state}. Ensure that the output include keys and query state '
                 f'keys you are trying to use with this key definition are reflective of both the input '
                 f'query state and the response query state. Not that aliases could also cause key match '
                 f'errors, ensure that key combinations are correct. Exception: {e}')

        logging.error(error)
        raise Exception(error)


def load_state(state_file: str) -> Any:
    state = {}
    if not os.path.exists(state_file):
        raise FileNotFoundError(
            f'state file {state_file} not found, please use the ensure to use the state template to create the input state file.')

    with open(state_file, 'r') as fio:
        state = json.load(fio)

        if 'config' not in state:
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


def build_template_text(template: dict, query_state: dict):
    if not template:
        warning = f'template is not set with query state {query_state}'
        logging.warning(warning)
        return False, None

    error = None
    if 'name' not in template:
        error = f'template name not set, please specify a template name for template {template}'

    if 'template_content' not in template:
        error = (f'template_content not specified, please specify the template_content for template {template} '
                 f'by either specifying the template_content or template_content_file as text or filename '
                 f'of the text representing the template {template}')

    if error:
        logging.error(error)
        raise Exception(error)

    # process the template now
    template_name = template['name']
    template_content = template['template_content']

    if 'parameters' not in template:
        logging.info(f'no parameters founds for {template_name}')
        return template_content

    # def replace_variable(match):
    #     variable_name = match.group(1)  # Extract variable name within {{...}}
    #     return query_state.get(variable_name, "{" + variable_name + "}")  # Replace or keep original
    #
    # completed_template = re.sub(r'\{(\w+)\}', replace_variable, template_content)

    completed_template = build_template_text_content(template_content=template_content,
                                                     query_state=query_state)

    return True, completed_template


def build_template_text_content(template_content: str, query_state: dict):
    def replace_variable(match):
        variable_name = match.group(1)  # Extract variable name within {{...}}
        return query_state.get(variable_name, "{" + variable_name + "}")  # Replace or keep original

    completed_template = re.sub(r'\{(\w+)\}', replace_variable, template_content)
    return completed_template


def parse_response_json(response: str):
    # TODO not ideal, but seems to kind of work
    _response = (response.replace('\n', '\\n')
                 .replace('{\\n', '{')
                 .replace('\\n}', '}')
                 .replace(',\\n', ',')
                 .replace('}\\n', '}'))

    try:
        json_response = json.loads(_response)
        json_response = {build_column_name(key): value for key, value in json_response.items()}
        return True, 'json', json_response
    except Exception as e:
        # ignore and move to the next step
        pass

    json_detect = _response.find('```json')
    if json_detect < 0:
        return False, type(response), response

    # find the first occurence of the json start {
    json_start = _response.find('{', json_detect)
    if json_start < 0:
        raise Exception(f'Invalid: json starting position not found, please ensure your response '
                        f'at position {json_detect}, for response {_response}')

    # we found the starting point, now we need to find the ending point
    json_end = _response.find('```', json_start)

    if json_end <= 0:
        raise Exception('Invalid: json ending position not found, please ensure your response is wrapped '
                        'with ```json\n{}\n``` where {} is the json response')

    json_response = _response[json_start:json_end].strip()
    try:
        json_response = json.loads(json_response)
        json_response = {build_column_name(key): value for key, value in json_response.items()}
        return True, 'json', json_response
    except:
        pass  # try one more time with no line returns

    try:
        json_response = json_response.replace('\\n', ' ')
        json_response = {build_column_name(key): value for key, value in json_response.items()}
        return True, 'json', json_response
    except:
        raise Exception(f'Invalid: json object even though we were able to extract it from the response text, '
                        f'the json response is still invalid, please ensure that json_response is correct, '
                        f'here is what we tried to parse into a json dictionary:\n{json_response}')

    return True, 'json', json_response


def parse_response_auto_detect_type(response: str):
    data_parse_status, data_type, data_parsed = parse_response_json(response=response)
    return data_parse_status, data_type, data_parsed


def parse_response(response: str):
    # try and identify and parse the response
    data_parse_status, data_type, data_parsed = parse_response_auto_detect_type(response)

    # if the parsed data is
    if data_parse_status:
        if 'json' == data_type:
            flattened = map_flattener.flatten(data_parsed)
            return flattened, data_type  # TODO extract the list of column names
        elif 'csv' == data_type:
            raise Exception(f'unsupported csv format, need to fix the _parse_response_csv(.) function')

    return response.strip(), data_type


def parse_response_strip_assistant_message(response: str):
    response, data_type = parse_response(response)

    if data_type is not str:
        return response, data_type

    remark_pos = response.find(':\n\n')
    if remark_pos >= 0:
        remark_pos = remark_pos + 3  # move forward 3 positions since thats what we searched for
        if remark_pos >= len(response):
            return response

        # else return characters after the has position
        return response[remark_pos:], data_type

    return response, data_type


def higher_order_routine_v2(func, **fixed_kwargs):
    def wrapped_function(*args, **kwargs):
        # Now accepts a positional argument
        all_kwargs = {**fixed_kwargs, **kwargs}
        return func(*args, **all_kwargs)

    return wrapped_function


def higher_order_routine(func, **fixed_kwargs):
    # The higher-order function
    def wrapped_function(**kwargs):
        # Merge fixed_kwargs (like header_input) with the new kwargs
        all_kwargs = {**fixed_kwargs, **kwargs}
        return func(**all_kwargs)

    return wrapped_function


def obsolete_parse_response_csv(response: str):
    ### TODO clean up this code
    ### TODO clean up this code
    ### TODO clean up this code

    # else do try deprecated method

    # Validate input format
    if '|' not in response or '\n' not in response:
        return response.strip()

    # Sanitize input
    response = response.strip()

    # split the data to rows by new line, and only fetch rows with '|' delimiter
    rows = [x for x in response.split('\n') if '|' in x]

    data = []
    columns = []

    # iterate each row found
    for index, row in enumerate(rows):
        row_data = [x.strip() for x in row.split('|') if len(x.strip()) > 0]
        has_row_data = [x for x in row_data if x != '' and not re.match(r'-+', x)]

        if not has_row_data:
            continue

        # must be header, hopefully
        # TODO should probably check against a schema requirement to ensure that the data that is produced meets the output requirements
        if index == 0:
            columns = row_data
            continue

        # make sure that the number of columns matches the number of data values, check on each row
        if len(row_data) != len(columns):
            error = (f'failed to process {row_data}, incorrect number of columns, '
                     f'expected {len(columns)} received: {len(row_data)} using response: {response}, '
                     f'with response: {response}')

            logging.error(error)
            raise Exception(error)

        record = {columns[i]: p for i, p in enumerate(row_data)}
        data.append(record)

    return data, columns
