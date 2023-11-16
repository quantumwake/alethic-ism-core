import json
import logging
import re
from typing import List

import pandas as pd
from tenacity import wait_exponential, retry, wait_random

from processor import map_flattener
from processor.base_processor import BaseProcessor


class BaseQuestionAnswerProcessor(BaseProcessor):

    @property
    def provider_name(self):
        return self.config['provider_name']

    @provider_name.setter
    def provider_name(self, provider_name: str):
        self.config['provider_name'] = provider_name

    @property
    def model_name(self):
        return self.config['model_name']

    @model_name.setter
    def model_name(self, model_name: str):
        self.config['model_name'] = model_name

    @property
    def user_template(self):
        return self.load_template(self.user_template_filename)

    @user_template.setter
    def user_template(self, template):
        self.config['user_template'] = template

    @property
    def system_template(self):
        return self.load_template(self.system_template_filename)

    @system_template.setter
    def system_template(self, template):
        self.config['system_template'] = template

    @property
    def user_template_filename(self):
        return self.config['user_template_file'] \
            if 'user_template_file' in self.config \
            else None

    @user_template_filename.setter
    def user_template_filename(self, filename: str):
        self.config['user_template_file'] = filename

    @property
    def system_template_filename(self):
        return self.config['system_template_file'] \
            if 'system_template_file' in self.config \
            else None

    @system_template_filename.setter
    def system_template_filename(self, filename: str):
        self.config['system_template_file'] = filename

    def __init__(self, name: str, config: dict, processors: List[BaseProcessor] = None, *args, **kwargs):
        super().__init__(name=name, config=config, processors=processors, **kwargs)
        # self.provider_name = config['provider_name']
        # self.model_name = config['model_name']
        # self.user_template = self.get_user_template()
        # self.system_template = self.get_system_template()

        self.output_dataframe = pd.DataFrame()  # TODO remove this or fix for incremental outputs

        # append header information
        header = {
            'header': {
                'provider_name': self.provider_name,
                'model_name': self.model_name,
                'system_template': self.system_template if self.system_template else None,
                'user_template': self.user_template if self.user_template else None
            }
        }

        self.state = {**header, **self.state}

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    def build_state_cache_filename(self):
        provider = ''.join([char for char in self.provider_name if char.isalnum()]).lower()
        model_name = ''.join([char for char in self.model_name if char.isalnum()]).lower()

        # if there is a system template file used, then the state file should include a suffix
        # this is useful if we want to change the persona of the system to respond from a specific perspective
        # such as the entities perspective or from another perspective such as Utilitarian Thinker.
        state_file_to_hash = f'{provider}_{model_name}'

        if self.system_template:
            _system_template_file = self.system_template_filename
            state_file_to_hash = f'{state_file_to_hash}_{_system_template_file}'

        if self.user_template:
            _user_template_file = self.user_template_filename
            state_file_to_hash = f'{state_file_to_hash}_{_user_template_file}'

        state_file_to_hash = f"[{self.name if self.name else '[Default]'}]/[{state_file_to_hash}]"
        state_file_hashed = self.get_hash_from_string(state_file_to_hash)
        state_file = f'{self.state_cache_path}/{state_file_hashed}.json'
        return state_file

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2))
    def call(self, values: dict, force: bool = False):
        if not values:
            return

        # query_value = values['query']
        query_state_key_hashed, query_state_key = self.get_query_state_key(
            query_state=values  # the individual state values of the input file
            # {"query": query_value}
        )

        if self.has_query_state(query_state_key=query_state_key_hashed, force=force):
            return

        status, user_prompt = self.build_template_text(self.user_template, values)
        status, system_prompt = self.build_template_text(self.system_template, values)

        try:
            response_data, response_columns = self._execute(user_prompt=user_prompt,
                                                            system_prompt=system_prompt,
                                                            values=values)

            logging.debug(f'processed prompt query: {query_state_key} and received response: {response_data}')

            query_state = {
                'key': query_state_key_hashed,
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
                'response': response_data,
                'status': 'Success'
            }

            # fetch any additional state from the input states, provided that the config_name parameter is set
            # otherwise it will default to the only parameter it knows of named 'query' within the input states
            additional_query_state = self.extract_values_from_query_state_by_key_config_name(
                config_name='output_include_states_details_from_input',
                query_state=values)

            # apply the additional states to the current query_state
            if additional_query_state:
                query_state = {
                    **query_state,
                    **additional_query_state
                }

            query_states = []
            if isinstance(response_data, dict):
                query_state = {**query_state, **response_data}
                query_states.append(query_state)
            elif isinstance(response_data, list):
                for item in list(response_data):
                    query_state = {**{
                        'user_prompt': user_prompt,
                        'system_prompt': system_prompt,
                        'status': 'Success'
                    }, **item, **additional_query_state}
                    query_states.append(query_state)
            else:
                query_states.append(query_state)

            def save_query_state(query_state: dict):
                # persist new state such that we do not repeat the call when interrupted
                self.save_state(key=query_state_key_hashed,
                                query_state=query_state)

                # persist the new record to the output file, if any
                self.write_record(query_state=query_state)
                return query_state

            query_states = [save_query_state(x) for x in query_states]

        except Exception as e:
            # save it as an error such that we can track it
            self.save_state_failure(key=query_state_key_hashed,
                                    query_state={
                                        'inputs': str(values),
                                        'user_prompt': user_prompt,
                                        'system_prompt': system_prompt,
                                        'response': f'error {e}',  # write the error to the answer for review purposes
                                        'status': 'Failed'
                                    })

            logging.error(f'critical error handling question {values} on {self.config}')

    def build_template_text(self, template: dict, query_state: dict):
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

        def replace_variable(match):
            variable_name = match.group(1)  # Extract variable name within {{...}}
            return query_state.get(variable_name, "{" + variable_name + "}")  # Replace or keep original

        completed_template = re.sub(r'\{(\w+)\}', replace_variable, template_content)
        return True, completed_template

    def _parse_response_json(self, response: str):
        json_detect = response.find('```json')
        if json_detect < 0:
            return False, type(response), response

        # find the first occurence of the json start {
        json_start = response.find('{', json_detect)
        if json_start < 0:
            raise Exception(f'Invalid: json starting position not found, please ensure your response '
                            f'at position {json_detect}, for response {response}')

        # we found the starting point, now we need to find the ending point
        json_end = response.find('```', json_start)

        if json_end <= 0:
            raise Exception('Invalid: json ending position not found, please ensure your response is wrapped '
                            'with ```json\n{}\n``` where {} is the json response')

        json_response = response[json_start:json_end].strip()
        try:
            json_response = json.loads(json_response)
            json_response = {utils.build(key): value for key, value in json_response.items()}
        except:
            raise Exception(f'Invalid: json object even though we were able to extract it from the response text, '
                            f'the json response is still invalid, please ensure that json_response is correct, '
                            f'here is what we tried to parse into a json dictionary:\n{json_response}')

        return True, 'json', json_response

    def _parse_response_auto_detect_type(self, response: str):
        data_parse_status, data_type, data_parsed = self._parse_response_json(response)
        return data_parse_status, data_type, data_parsed

    def _parse_response(self, response: str):
        # try and identify and parse the response
        data_parse_status, data_type, data_parsed = self._parse_response_auto_detect_type(response)

        # if the parsed data is
        if data_parse_status:
            if 'json' == data_type:
                flattened = map_flattener.flatten(data_parsed)
                return flattened, None  # TODO extract the list of column names
            elif 'csv' == data_type:
                raise Exception(f'unsupported csv format, need to fix the _parse_response_csv(.) function')

        return response.strip(), None

    def _parse_response_csv(self, response: str):
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
                         f'expected {len(columns)} received: {len(row_data)} using config {self.config}, '
                         f'with response: {response}')

                logging.error(error)
                raise Exception(error)

            record = {columns[i]: p for i, p in enumerate(row_data)}
            data.append(record)

        return data, columns

