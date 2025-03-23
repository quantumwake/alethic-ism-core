import hashlib
import json
import os
import re
import time
import uuid
from functools import wraps

import yaml
from typing import Union, Dict, Any, Optional

from ismcore.model.base_model import InstructionTemplate
from ismcore.utils.ism_logger import ism_logger
from ismcore.utils.map_utils import flatten

logging = ism_logger(__name__)

# only keep alphanumerical values and spaces, where spaces is converted to an underscore '_'
clean_char_for_ddl_naming = lambda x: (x.lower() if x.isalnum() or x == '_' else ' ' if x == '.' or x.isspace() else '')
clean_string_for_ddl_naming = lambda s: "_".join(''.join([clean_char_for_ddl_naming(c) for c in s]).split(' '))


def load_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data


def stopwatch(func):
    """Decorator to measure the execution time of a function."""
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()  # Start the stopwatch
        result = func(*args, **kwargs)
        end_time = time.perf_counter()    # Stop the stopwatch
        elapsed_time = end_time - start_time
        print(f"Function '{func.__name__}' executed in {elapsed_time:.6f} seconds.")
        return result
    return wrapper


def async_stopwatch(func):
    """Decorator to measure the execution time of an async function."""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        start_time = time.perf_counter()  # Start the stopwatch
        result = await func(*args, **kwargs)
        end_time = time.perf_counter()    # Stop the stopwatch
        elapsed_time = end_time - start_time
        logging.debug(f"async function '{func.__name__}' executed in {elapsed_time:.6f} seconds.")
        return result
    return wrapper


def merge_nested_dicts(d1, d2):
    for key, value in d2.items():
        if key in d1 and isinstance(d1[key], dict) and isinstance(value, dict):
            merge_nested_dicts(d1[key], value)
        else:
            d1[key] = value

    return d1


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


def has_extension(filename: str, extensions: [str]):
    filename = filename.lower()
    return any(filename.endswith(ext) for ext in extensions)


def load_template(template_config_file: str):
    if not template_config_file or not os.path.exists(template_config_file):
        return None

    with open(template_config_file, 'r') as fio:
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
            raise ValueError(f'Cannot define a template_content and a template_content_file in the same '
                            f'template configuration {template_config_file}. For example, if you define the key '
                            f'"template_content" with some text content, you cannot also define a '
                            f'"template_content_file" which points to a different content from a file"')

        # otherwise load the template
        template_content_file = template_dict['template_content_file']

        if not os.path.exists(template_content_file):
            logging.debug(f'unable to find content file with path {template_content_file}, attempting to use configuration path {template_config_file}')
            # extract relative or absolute path
            path = os.path.dirname(template_config_file)
            template_content_file = f'{path}/{template_content_file}'

            # second attempt, we'll use the configuration file path ass a prefix
            if not os.path.exists(template_content_file):
                logging.warning(f'unable to load content file with path {template_content_file}, '
                                f'last attempt to try and figure out path of file')

                template_content_file = f'{path}/{os.path.basename(template_content_file)}'

        # last check to ensure file exists
        if not os.path.exists(template_content_file):
            error = f'critical error, unable to load content file with path {template_content_file}'
            raise FileNotFoundError(error)

        with open(template_content_file, 'r') as fio_tc:
            template_content = fio_tc.read().strip()
            template_dict['template_content'] = template_content

    if not template_content:
        raise ValueError(f'no template defined in template file {template_config_file} with configuration {template_dict}')

    return template_dict



def calculate_sha256(input_string: str):
    # Create a SHA-256 hash object
    hash_object = hashlib.sha256()

    # Update the hash object with the bytes of the string
    hash_object.update(input_string.encode())

    # Get the hexadecimal representation of the hash
    return hash_object.hexdigest()


def calculate_uuid_based_from_string_with_sha256_seed(input_string: str) -> str:
    """Generate a UUID based on the SHA-256 hash of the input string."""
    hash_bytes = hashlib.sha256(input_string.encode('utf-8')).digest()

    # Convert the hash bytes to a large integer
    hash_int = int.from_bytes(hash_bytes, byteorder='big')

    # Modulo by 2^128 to ensure it fits in 128 bits
    hash_int = hash_int % (1 << 128)

    # Create a UUID using the adjusted integer
    return str(uuid.UUID(int=hash_int))


def calculate_string_list_hash(names: [str]):
    plain = ",".join([f'({idx}:{key})' for idx, key in enumerate(sorted(names))])
    return calculate_uuid_based_from_string_with_sha256_seed(plain)


def calculate_string_dict_hash(item: dict):
    plain = ",".join([f'({key}:{value})' for key, value in item.items()])
    return calculate_uuid_based_from_string_with_sha256_seed(plain)


def build_template_text_mako(template: [str, dict], data: dict, error_callback: callable = None) -> Optional[str]:
    if not template:
        warning = f'called build template but template is not set'
        logging.warning(warning)
        return None

    content = get_template_content(template)

    # mako template processing
    try:
        from mako.template import Template
        from mako.runtime import Context
        from io import StringIO

        # Create a Template object
        mako_template = Template(content, error_handler=error_callback)

        # Render the template with the data
        result = mako_template.render(**data)
        return result
    except Exception as e:
        error = f'failed to process mako template {template} with error: {e}'
        logging.error(error)
        raise ValueError(error)


def get_template_content(template: Union[dict, str]):
    if isinstance(template, str):
        return template

    if isinstance(template, dict):
        error = None
        if 'name' not in template:
            error = f'template name not set, please specify a template name for template {template}'

        if 'template_content' not in template:
            error = (f'template_content not specified, please specify the template_content for template {template} '
                     f'by either specifying the template_content or template_content_file as text or filename '
                     f'of the text representing the template {template}')

        if error:
            logging.error(error)
            raise ValueError(error)

        # process the template now
        template_name = template['name']
        template_content = template['template_content']

        if 'parameters' not in template:
            logging.info(f'no parameters founds for {template_name}')
            return template_content

        return template_content

    raise NotImplementedError(f'template of type {type(template)} not supported')


def build_template_text_v2(template: InstructionTemplate, data: dict, error_callback: callable = None) -> Optional[str]:
    if not template:
        return str(data)

    if template.template_type == "mako":
        return build_template_text_mako(
            template=template.template_content,
            data=data, error_callback=error_callback)
    elif template.template_type == "jinja2":
        raise NotImplementedError('jinja2 not implemented yet')
        # return build_template_text_jinja2(template=template.template_content, data=data)
    elif template.template_type == "python":
        return build_template_text_mako(
            template=template.template_content,
            data=data, error_callback=error_callback)
    elif template.template_type == "simple" or not template.template_type:
        return build_template_text(
            template=template.template_content,
            query_state=data
        )


def build_template_text(template: Union[dict, str], query_state: dict, strip_newlines: bool = True) -> Optional[str]:

    if not template:
        warning = f'template is not set with query state {query_state}'
        logging.warning(warning)
        return None

    # get the template content
    content = get_template_content(template)

    # process the template text
    completed_template = build_template_text_content(
        template_content=content,
        query_state=query_state,
        strip_newlines=strip_newlines)

    return completed_template

#
# def build_template_text_content(template_content: str, query_state: dict, strip_newlines: bool = True):
#     def replace_variable(match):
#         variable_name = match.group(1)  # Extract variable name within {{...}}
#         if variable_name not in query_state and 'justification' in variable_name:
#
#             # TODO we need to evaluate whether this key is optional or mandatory
#             #  for now we just hard code this key "justification" as an optional parameter
#             return ""
#
#         # otherwise throw an exception or fetch the variable
#         value = query_state.get(variable_name, "{" + variable_name + "}")  # Replace or keep original
#         if isinstance(value, str) and strip_newlines:
#             value = value.replace('\\n', '\n').replace('\n', ' ')
#
#         return value  # Replace or keep original
#
#     completed_template = re.sub(r'\{(\w+)\}', replace_variable, template_content)
#     return completed_template

#
# def build_template_text_jinja2(template_content: str, data: Dict[str, Any]) -> str:
#     # Create a Template object
#     template = Template(template_content)
#
#     # Render the template with the data
#     result = template.render(data)
#
#     return result


def build_template_text_content(
        template_content: str,
        query_state: Dict[str, Any],
        strip_newlines: bool = True,
        optional_fields: set = {}
) -> str:
    def process_value(value: Any) -> str:
        if isinstance(value, str) and strip_newlines:
            return value.replace('\\n', '\n').replace('\n', ' ')
        return str(value)

    def replace_variable(match: re.Match) -> str:
        variable_name = match.group(1)  # Extract variable name within {{...}}

        if variable_name not in query_state:
            if variable_name in optional_fields:
                return ""
            return "{" + variable_name + "}"

        value = query_state[variable_name]
        return process_value(value)

    completed_template = re.sub(r'\{(\w+)\}', replace_variable, template_content)
    return completed_template


def parse_response_json(response: str):
    # TODO not ideal, but seems to kind of work
    _response = (response.replace('\n', '\\n')
                 .replace('{\\n', '{')
                 .replace('\\n}', '}')
                 .replace('",\\n', '",')
                 .replace('},\\n', '},')
                 .replace('\\n,{', ',{')
                 .replace('}\\n', '}')
                 .replace('\\n{', '{')
                 .replace('"\\n', '"')
                 .replace('\\n"', '"')
                 .replace('[\\n', '[')
                 .replace('\\n[', '[')
                 .replace('\\n]', ']')
                 .replace(']\\n', ']'))

    try:
        json_response = json.loads(_response)
        json_response = {clean_string_for_ddl_naming(key): value for key, value in json_response.items()}
        return True, 'json', json_response
    except Exception as e:
        # ignore and move to the next step
        pass

    json_detect = _response.find('```json')
    if json_detect < 0:
        return False, type(response), response

    # find the first occurrence of the json start [ or {
    json_start_array = _response.find('[', json_detect)
    json_start_object = _response.find('{', json_detect)
    if json_start_array != -1 and json_start_array < json_start_object:
        json_start = json_start_array
    else:
        json_start = json_start_object

    if json_start < 0:
        raise SyntaxError(f'Invalid: json starting position not found, please ensure your response '
                          f'at position {json_detect}, for response {_response}')

    # we found the starting point, now we need to find the ending point
    json_end = _response.find('```', json_start)

    if json_end <= 0:
        raise SyntaxError('Invalid: json ending position not found, please ensure your response is wrapped '
                          'with ```json\n{}\n``` where {} is the json response')

    json_response = _response[json_start:json_end].strip()
    try:
        json_response = json.loads(json_response)
    except:
        # try a different approach by stripping out the \\n
        try:
            json_response = json_response.replace('\\n', ' ')
            json_response = json.loads(json_response)
        except:
            raise SyntaxError(f'Invalid: json object even though we were able to extract it from the response text, '
                              f'the json response is still invalid, please ensure that json_response is correct, '
                              f'here is what we tried to parse into a json dictionary:\n{json_response}')

    # checks response type
    if isinstance(json_response, dict):
        json_response = {
            clean_string_for_ddl_naming(key): value
            for key, value in json_response.items()
        }
    elif isinstance(json_response, list):
        json_response = [
            {
                clean_string_for_ddl_naming(key): value
                for key, value in row.items()
            }
            for row in json_response
        ]

    return True, 'json', json_response

def parse_response_auto_detect_type(response: str):
    data_parse_status, data_type, data_parsed = parse_response_json(response=response)
    return data_parse_status, data_type, data_parsed


def parse_response(raw_response: str):

    if raw_response:
        raw_response = raw_response.strip()

    # try and identify and parse the response
    data_parse_status, data_type, data_parsed = parse_response_auto_detect_type(raw_response)

    # if the parsed data is
    if data_parse_status:
        if 'json' == data_type:
            flattened = flatten(data_parsed)
            return flattened, data_type, raw_response  # TODO extract the list of column names
        elif 'csv' == data_type:
            raise ValueError(f'unsupported csv format, need to fix the _parse_response_csv(.) function')

    return raw_response, data_type, raw_response


def parse_response_strip_assistant_message(raw_response: str):
    response, data_type, raw_response = parse_response(raw_response)

    # TODO clean this up?

    if data_type is not str:
        return response, data_type, raw_response

    remark_pos = response.find(':\n\n')
    if remark_pos >= 0:
        remark_pos = remark_pos + 3  # move forward 3 positions since thats what we searched for
        if remark_pos >= len(response):
            return response, data_type, raw_response

        # else return characters after the has position
        return response[remark_pos:], data_type, raw_response

    return response, data_type, raw_response


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
            raise ValueError(error)

        record = {columns[i]: p for i, p in enumerate(row_data)}
        data.append(record)

    return data, columns
