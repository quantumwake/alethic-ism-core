import logging
import re
from typing import List, Any

import os.path
import json
import openai
import pandas as pd
import hashlib
import dotenv

from anthropic import Anthropic, HUMAN_PROMPT, AI_PROMPT
from tenacity import retry, wait_exponential, wait_random

dotenv.load_dotenv()
openai_api_key = os.environ.get('OPENAI_API_KEY', None)
openai.api_key = openai_api_key

LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging.basicConfig(filename=LOG_FILE, encoding='utf-8', level=LOG_LEVEL)


class BaseQuestionAnswerProcessor:

    def __init__(self, config: dict):
        self.state = {
            'config': config,
            'data': list(),
            'mapping': dict(),
        }

        self.provider_name = config['provider_name']
        self.model_name = config['model_name']
        self.user_template = self.get_user_template()
        self.system_template = self.get_system_template()
        self.output_dataframe = pd.DataFrame()

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

        # try and load the state if it exists already
        self.load_state()
        self.dump_cache()

    @property
    def config(self):
        return self.state['config']

    def add_output_from_dict(self, query_state: dict):
        if len(self.output_dataframe) > 0:
            _append_query_state = pd.DataFrame([query_state])
            self.output_dataframe = pd.concat([self.output_dataframe, _append_query_state])
        else:
            self.output_dataframe = pd.DataFrame([query_state])

    def load_template_content(self, template_file: str):
        if not template_file or not os.path.exists(template_file):
            return None

        with open(template_file, 'r') as fio:
            return json.load(fio)

    def get_system_template_filename(self):
        return self.config['system_template_file'] \
            if 'system_template_file' in self.config \
            else None

    def get_state_cache_path(self):
        return self.config['state_cache_path'] \
            if 'state_cache_path' in self.config \
            else None

    def get_hash_from_string(self, input_string: str):
        # Create a SHA-256 hash object
        hash_object = hashlib.sha256()

        # Update the hash object with the bytes of the string
        hash_object.update(input_string.encode())

        # Get the hexadecimal representation of the hash
        return hash_object.hexdigest()

    def tt(self, config_name: str, query_state: dict):
        if config_name not in self.config:
            if "query" not in query_state:
                raise Exception(f'query does not exist in query state {query_state}')

            return query_state['query']

        # basically a list of parameter names to pick off the query state object
        key_definition = self.config[config_name]

        # iterate each parameter name and look it up in the state object
        results = {}
        for name in key_definition:

            alias = name
            # check to make sure that this is not a dict
            if isinstance(name, dict):
                # if dictionary then we must strip out the name and alias name
                # this is used to create a target dictionary from the source dictionary
                # but using a new alias, as defined by the config values
                #    'config_name':[{ 'query': 'value'}, ...]
                keyval = dict(name)
                if len(keyval) != 1:
                    raise Exception(f'Invalid key value pair set for config_name {config_name}, values {keyval} is invalid, only a single key value pair is allowed when defining alias')

                # since there should only be 1 value, pop it and read it in
                keyval = keyval.popitem()
                name = keyval[0]        # the new name is no longer the dict but the key name in the dict
                alias = keyval[1]       # the alias is not the name but pulled from the dict key value

            # if it does not exist, throw an exception to warn the user that the state input is incorrect.
            if name not in query_state:
                raise Exception(f'Invalid state input for parameter: {name}, '
                                f'query_state: {query_state}, '
                                f'key definition: {key_definition}')

            # add a new key state value pair,
            # constructing a key defined by values from the query_state
            # given that the key exists and is defined in the config_name mapping
            results[alias] = query_state[name]

        return results

    def get_query_state_key(self, query_state: dict):

        state_values = self.tt(config_name='state_query_key_definition',
                               query_state=query_state)

        # if 'state_query_key_definition' not in self.config:
        #     return "query"
        #
        # # basically a list of parameter names to pick off the query state object
        # key_definition = self.config['state_query_key_definition']
        #
        # # iterate each parameter name and look it up in the state object
        # key = ""
        # for name in key_definition:
        #
        #     # if it does not exist, throw an exception to warn the user that the state input is incorrect.
        #     if name not in query_state:
        #         raise Exception(f'Invalid state input for parameter: {name}, '
        #                         f'query_state: {query_state}, '
        #                         f'key definition: {key_definition}')
        #
        #     # append the key state value, constructing a key defined by values from the query_state
        #     value = query_state[name]
        #     key = f"{key}:{value}"

        keys = [(name, value) for name, value in state_values.items()]

        return self.get_hash_from_string(str(keys)), keys

    def get_state_cache_filename(self):
        provider = ''.join([char for char in self.provider_name if char.isalnum()]).lower()
        model_name = ''.join([char for char in self.model_name if char.isalnum()]).lower()

        # if there is a system template file used, then the state file should include a suffix
        # this is useful if we want to change the persona of the system to respond from a specific perspective
        # such as the entities perspective or from another perspective such as Utilitarian Thinker.
        state_file_to_hash = f'{provider}_{model_name}'

        if self.system_template:
            _system_template_file = self.get_system_template_filename()
            state_file_to_hash = f'{state_file_to_hash}_{_system_template_file}'

        if self.user_template:
            _user_template_file = self.get_user_template_filename()
            state_file_to_hash = f'{state_file_to_hash}_{_user_template_file}'

        state_file_hashed = self.get_hash_from_string(state_file_to_hash)
        state_file = f'{self.get_state_cache_path()}/{state_file_hashed}.json'
        return state_file

    def get_user_template_filename(self):
        return self.config['user_template_file'] \
            if 'user_template_file' in self.config \
            else None

    def get_system_template(self):
        return self.load_template_content(self.get_system_template_filename())

    def get_user_template(self):
        return self.load_template_content(self.get_user_template_filename())

    def get_output_filename(self):
        return self.config['output_file'] \
            if 'output_file' in self.config \
            else None

    def get_system_template_parameters(self):
        if self.user_template:
            raise NotImplementedError()

    def get_user_template_parameters(self):
        if self.system_template:
            raise NotImplementedError()


    def batching(self, questions: List[str]):
        pass

    def has_query_state(self, query_state_key: str, force: bool = False):
        # make sure that the state is initialized and that there is a data key
        if not self.state or not self.state['mapping']:
            return None

        mapping = self.state['mapping']
        if not force and query_state_key in mapping:  # skip if not forced and state exists
            logging.info(f'query {query_state_key}, cached, on config: {self.config}')
            return True

        # otherwise return none, which means no state exists
        logging.info(f'query {query_state_key}, not cached, on config: {self.config}')
        return False

    def process(self):
        input_file = self.config['input_file']
        output_file = self.config['output_file']

        if not os.path.exists(input_file):
            raise FileNotFoundError(
                f'query dataset file {input_file} not found, please use the question.csv template to create a single column question csv.')

        dump_on_every_call = bool(self.config['dump_on_every_call']) \
            if 'dump_on_every_call' in self.config \
            else False

        if '.json' in input_file.lower():
            with open(input_file, 'r') as fio:
                dataset = json.load(fio)
                dataset = dataset['data']

            for data in dataset:
                self.call(values=data)

                if dump_on_every_call:
                    self.dump_dataframe_csv()
        elif '.csv' in input_file.lower():
            raise NotImplementedError('not working')
            #
            # df = self.load_datset_file(input_file)
            # for row in df.itertuples():
            #     self.call(values=row)
            #     # if dump_on_every_call:
                    # self.dump()

            return df

    def get_model_name(self):
        return self.model_name

    def get_provider(self):
        return self.provider_name

    def load_state(self):
        state_file = self.get_state_cache_filename()
        if not os.path.exists(state_file):
            return self.state

        with open(state_file, 'r') as fio:
            self.state = json.load(fio)
            return self.state

    def save_state(self, key: str, query_state: dict):
        state_file = self.get_state_cache_filename()

        # update the data states for the specific query / question / input
        data = self.state['data']
        data.append(query_state)

        # store a hashed value, not the best method I admit,
        # but it will do for now we are not dealing with massive data, yet
        # TODO IMPORTANT - PERFORMANCE AND STORAGE
        # TODO look into this potential performance and storage bottleneck,
        # TODO probably would benefit from a database backend instead
        # key = self.get_query_state_key(query_state)
        mapping = self.state['mapping']
        mapping[key] = query_state

        with open(state_file, 'w') as fio:
            json.dump(self.state, fio)

        return self.state

    def load_datset_file(self, file: str):
        _file = file.lower()
        if _file.endswith('.csv'):
            return pd.read_csv(file)
        elif _file.endswith('.xlsx') or _file.endswith('.xls'):
            return pd.read_excel(file)

        return None

    def write_record(self, query_state: str):
        data = self.state['mapping'] if 'mapping' in self.state else None

        if not data:
            error = f'no data mapping found in query state {query_state} of {self.config} for {self}'
            logging.error(error)
            raise Exception(error)

        self.add_output_from_dict(query_state=query_state)

    def dump_dataframe_csv(self):
        # not safe for consistency
        self.output_dataframe.to_csv(self.get_output_filename())

    def dump_cache(self):
        # safe for consistency
        output_filename = self.get_output_filename()

        if not output_filename:
            logging.error(f'unable to persist to csv output file, output_filename is not set')

        if '.csv' in output_filename:
            self.dump_cache_csv()
        elif '.json' in output_filename:
            self.dump_cache_json()
        else:
            logging.error(f'unsupported output file')

    def dump_cache_csv(self):
        # safe for consistency

        output_filename = self.get_output_filename()

        if output_filename:
            data = self.state['data']
            df = pd.DataFrame(data)
            df.to_csv()
        else:
            logging.error(f'unable to persist to csv output file, output_filename is not set')

    def dump_cache_json(self):
        output_filename = self.get_output_filename()

        if not output_filename:
            logging.error(f'unable to persist to csv output file, output_filename is not set')

        with open(output_filename, 'w') as fio:
            json.dump(self.state, fio)

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2))
    def call(self, values: dict, force: bool = False):

        if not values:
            return

        #query_value = values['query']
        query_state_key_hashed, query_state_key = self.get_query_state_key(
            query_state=values          # the individual state values of the input file
            # {"query": query_value}
        )

        if self.has_query_state(query_state_key=query_state_key_hashed, force=force):
            return

        status, user_prompt = self.build_template_text(self.user_template, values)
        status, system_prompt = self.build_template_text(self.system_template, values)

        try:
            response = self._execute(user_prompt=user_prompt,
                                     system_prompt=system_prompt,
                                     values=values)


            logging.debug(f'processed prompt query: {query_state_key} and received response: {response}')

            query_state = {
                'key': query_state_key_hashed,
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
                'response': response,
                'status': 'Success'
            }

            # fetch any additional state from the input states, provided that the config_name parameter is set
            # otherwise it will default to the only parameter it knows of named 'query' within the input states
            additional_query_state = self.tt(config_name='output_include_states_details_from_input',
                                             query_state=values)

            # apply the additional states to the current query_state
            if additional_query_state:
                query_state = {
                    **query_state,
                    **additional_query_state
                }

            query_states = []
            if isinstance(response, dict):
                query_state = {**query_state, **response}
                query_states.append(query_state)
            elif isinstance(response, list):
                for item in list(response):
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
            self.save_state(key=query_state_key_hashed,
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

        if 'template' not in template:
            error = f'template text not specified, please specify the template text for template {template}'

        if error:
            logging.error(error)
            raise Exception(error)

        # process the template now
        template_name = template['name']
        template_text = template['template']

        if 'parameters' not in template:
            logging.info(f'no parameters founds for {template_name}')
            return template_text

        def replace_variable(match):
            variable_name = match.group(1)  # Extract variable name within {{...}}
            return query_state.get(variable_name, "{" + variable_name + "}")  # Replace or keep original

        completed_template = re.sub(r'\{(\w+)\}', replace_variable, template_text)
        return True, completed_template


class AnthropicBaseProcessor(BaseQuestionAnswerProcessor):

    def __init__(self, config: dict):
        new_config = {**{'provider_name': "Anthropic", 'model_name': "claude-2"}, **config}
        super().__init__(config=new_config)

        self.anthropic = Anthropic(max_retries=5)

    def batching(self, questions: List[str]):
        raise NotImplementedError()

    def _parse_response(self, response: str):
        if not response or response.strip() == '':
            logging.error(f'no response found for config {self.config} on {self}')
            return response

        result = ' '.join([x.strip() for x in response.split('\n\n') if x.strip() != '' and not x.strip().endswith(":")])
        return result          # empty columns, text parsing only

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        # add a system message if one exists
        final_prompt = f"{HUMAN_PROMPT} {user_prompt} {AI_PROMPT}"
        if system_prompt:
            final_prompt = f'{system_prompt} {final_prompt}'

        # strip out any white spaces and execute the final prompt
        final_prompt = final_prompt.strip()
        completion = self.anthropic.completions.create(
            model="claude-2",
            max_tokens_to_sample=1024,
            prompt=final_prompt,
        )

        response = completion.completion
        return response

class AnthropicQuestionAnswerProcessor(AnthropicBaseProcessor):

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        response = super()._execute(user_prompt=user_prompt, system_prompt=system_prompt, values=values)
        response = self._parse_response(response=response)
        return response


class AnthropicQuestionResponsePerspectiveProcessor(AnthropicBaseProcessor):

    def __init__(self, config: dict):
        new_config = {**{'provider_name': "Anthropic", 'model_name': "claude-2"}, **config}
        super().__init__(config=new_config)

        self.anthropic = Anthropic(max_retries=5)

    def batching(self, questions: List[str]):
        raise NotImplementedError()

    def _parse_response(self, response: str):
        # Validate input format
        if '|' not in response or '\n' not in response:
            raise ValueError('Invalid response format')

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

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        response = super()._execute(user_prompt=user_prompt, system_prompt=system_prompt, values=values)
        data, columns = self._parse_response(response)
        return data



class OpenAIQuestionAnswerProcessor(BaseQuestionAnswerProcessor):

    def __init__(self):
        super().__init__({
            'provider_name': "OpenAI",
            'model_name': "gpt-4-1106-preview"
        })

    def batching(self, questions: List[str]):
        raise NotImplementedError()

    def call(self, question, force: bool = False):
        if self.has_query_state(query=question, force=force):
            return

        # otherwise process the question
        messages_dict = [
            {
                "role": "user",
                "content": f"{question}"
            }
        ]

        # if there is a system template, then append it to the input vector
        if self.system_template:
            messages_dict.append({
                "role": "system",
                "content": self.system_template
            })

        try:
            # execute the open ai api function and wait for the response
            response = openai.ChatCompletion.create(
                model=self.model_name,
                messages=messages_dict,
                temperature=0.8,
                # TODO - IMPORTANT test this as it will likely have an impact on how the system responds
                max_tokens=1024
            )

            # strip out the relevant text information

            answer = response.choices[0]['message']['content'].strip().replace('\n', ' ').replace('  ', ' ')
            self.state[question] = {
                'answer': answer,
                'status': 'Success'
            }
            self.save_state()  # persist new state such that we do not repeat the call when interrupted
        except Exception as e:
            self.state[question] = {
                'answer': f'error {e}',  # write the error to the answer for review purposes
                'status': 'Failed'
            }
            logging.error(f'critical error handling question {question} on {self}')



def test_sg_questions_4gs():
    config = {
        # where to store the state
        'state_cache_path': '../dataset/examples/states',
        'dump_on_every_call': True,

        # the output file key used when generating new output values
        'state_query_key_definition': [
            'query',
            'context'
        ],

        'output_include_states_details_from_input': [
            {'query': 'input_query'},
            {'context': 'input_context'}
        ]
    }

    locations = {
        # the input questions and other parameters
        'input_file': '../dataset/examples/processor/vetted_questions/sg/questions_4gs.json',
        'output_file': '../dataset/examples/processor/vetted_questions/questions_4gs_response_output.json',

        # templates to use
        'system_template_file': '../dataset/examples/processor/questions/questions_with_context_system_template.json',
        'user_template_file': '../dataset/examples/processor/questions/questions_with_context_user_template.json',
    }

    anthropic_processor = AnthropicQuestionAnswerProcessor(config={**config, **locations})
    anthropic_processor.process()


def test_sg_questions_4gs_perspective():
    config = {
        # where to store the state
        'state_cache_path': '../dataset/examples/states',
        'dump_on_every_call': True,

        # the output file key used when generating new output values
        'state_query_key_definition': [
            'input_query',
            'input_context',
            # { 'name': 'perspective', "dynamic": True }
        ],

        'output_include_states_details_from_input': [
            {'input_query': 'input_query_original'},
            {'input_query': 'input_query_original'}
        ]
    }

    locations = {
        # the input questions and other parameters
        'input_file': '../dataset/examples/processor/vetted_questions/sg/questions_4gs.json',
        'output_file': '../dataset/examples/processor/vetted_questions/questions_4gs_response_output.json',

        # templates to use
        'system_template_file': '../dataset/examples/processor/questions/questions_with_context_system_template.json',
        'user_template_file': '../dataset/examples/processor/questions/questions_with_context_user_template.json',

    }

    anthropic_processor = AnthropicQuestionAnswerProcessor(config={**config, **locations})
    anthropic_processor.process()

# main function
if __name__ == '__main__':
    # test_sg_questions_4gs()

    # anthropic_processor = AnthropicQuestionAnswerProcessor(config={
    #     'input_file': '../dataset/examples/processor/questions/questions.json',
    #     'output_file': '../dataset/examples/processor/questions/questions_output.json',
    #     'system_template_file': '../dataset/examples/processor/questions/questions_system_template.json',
    #     'user_template_file': '../dataset/examples/processor/questions/questions_user_template.json',
    #     'state_cache_path': '../dataset/examples/states',
    #     'dump_on_every_call': True
    # })
    #
    # anthropic = anthropic_processor.process()
    # anthropic_processor.dump_cache()

    anthropic_perspective_processor = AnthropicQuestionResponsePerspectiveProcessor(config={
        'input_file': '../dataset/examples/processor/questions/questions_output.json',
        'output_file': '../dataset/examples/processor/perspective/perspective_output.csv',
        # 'system_template_file': '../dataset/examples/processor/perspective/perspective_system_template.json',
        'user_template_file': '../dataset/examples/processor/perspective/perspective_user_template_v2.json',
        'state_cache_path': '../dataset/examples/states',
        # the output file key used when generating new output values
        'state_query_key_definition': [
            'query'
        ],
    })

    #
    anthropic = anthropic_perspective_processor.process()
    anthropic_perspective_processor.dump_cache_csv()

    # questions_input_file='../dataset/examples/processor/questions/questions.csv',
    # questions_answers_output_file='../dataset/examples/processor/questions/question_answer_response.csv')

    # openai = OpenAIQuestionAnswerProcessor()
    # openai = processor.process(
    #     questions_input_file='../dataset/examples/processor/questions/questions.csv',
    #     questions_answers_output_file='../dataset/examples/processor/questions/question_answer_response.csv')
    # print(openai)
