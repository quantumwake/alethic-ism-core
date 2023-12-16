import logging as log
from typing import List

from .processor_state import (
    State,
    StateConfigLM,
    build_state_data_row_key,
    extract_values_from_query_state_by_key_definition)

from .base_processor import BaseProcessor

from .utils.general_utils import (
    load_template,
    build_template_text,
    calculate_string_dict_hash,
    clean_string_for_ddl_naming
)

logging = log.getLogger(__name__)


class BaseQuestionAnswerProcessor(BaseProcessor):

    @property
    def config(self) -> StateConfigLM:
        return self.state.config

    @property
    def provider_name(self):
        return self.config.provider_name

    @provider_name.setter
    def provider_name(self, provider_name: str):
        self.config.provider_name = provider_name

    @property
    def model_name(self):
        return self.config.model_name

    @model_name.setter
    def model_name(self, model_name: str):
        self.config.model_name = model_name

    @property
    def user_template(self):
        return load_template(self.user_template_path)

    @property
    def system_template(self):
        return load_template(self.system_template_path)

    @property
    def user_template_path(self):
        return self.config.user_template_path

    @user_template_path.setter
    def user_template_path(self, path: str):
        self.config.user_template_path = path

    @property
    def system_template_path(self):
        return self.config.system_template_path

    @system_template_path.setter
    def system_template_path(self, path: str):
        self.config.system_template_path = path

    def __init__(self, state: State, processors: List[BaseProcessor] = None, *args, **kwargs):
        super().__init__(state=state, processors=processors, **kwargs)

        # ensure that the configuration passed is of StateConfigQuestionAnswer
        if not isinstance(self.state.config, StateConfigLM):
            raise ValueError(f'invalid state config, '
                             f'got {type(self.state.config)}, '
                             f'expected {StateConfigLM}')

        logging.info(f'starting instruction state machine: {type(self)} with config {self.config}')

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    def build_final_output_path(self):
        provider = ''.join([char for char in self.provider_name if char.isalnum()]).lower()
        model_name = ''.join([char for char in self.model_name if char.isalnum()]).lower()
        user_template = self.user_template          # this will be hashed
        system_template = self.system_template      # this will be hashed

        # return super().build_final_output_path(prefix=f'{provider}_{model_name}')
        return super().build_final_output_path(prefix=f'{provider}_{model_name}_[system template: {system_template}]_[user template: {user_template}]')


    def apply_states(self, query_states: [dict]):
        with self.lock:
            logging.debug(f'persisting processed new query states from response. query states: {query_states} ')
            def save_query_state(query_state: dict):

                # persist new state such that we do not repeat the call when interrupted
                query_state = self.apply_query_state(query_state=query_state)

                # persist the new record to the output file, if any
                # TODO STREAM IT self.write_record(query_state=query_state)

                return query_state

            return [save_query_state(x) for x in query_states]

    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        if not input_query_state:
            return

        # create the query data row primary key hash
        # the individual state values of the input file
        input_state_key, query_state_key = build_state_data_row_key(
            # we need the input query to create the primary key
            query_state=input_query_state,

            # we use the primary key definition because we are creating a primary key
            key_definitions=self.state.config.primary_key
        )

        # if it already exists, then skip it, unless forced
        if self.has_query_state(query_state_key=input_state_key, force=force):
            return

        # build out the final prompts with appropriate data injected
        status, user_prompt = build_template_text(self.user_template, input_query_state)
        status, system_prompt = build_template_text(self.system_template, input_query_state)

        try:

            # execute the underlying model function
            response_data, response_columns, full_response = (
                self._execute(user_prompt=user_prompt,
                              system_prompt=system_prompt,
                              values=input_query_state))

            logging.debug(f'processed prompt query: {query_state_key} and received response: {response_data}')

            # we build a new output state to be appended to the output states
            output_query_state = {
                'state_key': input_state_key,
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
                'response': response_data,
                'raw_response': full_response if full_response else '<<<blank>>>',
                'status': 'Success'
            }

            # fetch any additional state from the input states, provided that the config_name parameter is set
            # otherwise it will default to the only parameter it knows of named 'query' within the input states
            additional_query_state = extract_values_from_query_state_by_key_definition(
                key_definitions=self.query_state_inheritance,
                query_state=input_query_state)

            # apply the additional states to the current query_state
            if additional_query_state:
                output_query_state = {
                    **output_query_state,
                    **additional_query_state
                }

            query_states = []
            if isinstance(response_data, dict):
                output_query_state = {**output_query_state, **response_data}
                query_states.append(output_query_state)
            elif isinstance(response_data, list):
                for item in list(response_data):
                    if not isinstance(item, dict):
                        raise Exception(f'item in response list of rows must be in of type dict.')

                    state_item_key = calculate_string_dict_hash(item)

                    #
                    output_query_state = {**{
                        'state_key': input_state_key,
                        'state_item_key': state_item_key,
                        'user_prompt': user_prompt,
                        'system_prompt': system_prompt,
                        'status': 'Success'
                    }, **item, **additional_query_state}

                    # format the keys, stripping the key name to something more generalized
                    output_query_state = {clean_string_for_ddl_naming(key): value
                                          for key, value
                                          in output_query_state.items()}

                    query_states.append(output_query_state)
            else:
                query_states.append(output_query_state)

            # write the query states (synchronized)
            query_states = self.apply_states(query_states=query_states)

            # return the updated query state after persistence, generally the same unless it was remapped to new columns
            return query_states

        except SyntaxError as e:
            logging.error(f'unable to parse response, skipping question {input_query_state} on {self.config}')
        except Exception as e:
            logging.error(f'critical error handling question {input_query_state} on {self.config} client gave up')







