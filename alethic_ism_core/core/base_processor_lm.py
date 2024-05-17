import logging as log
from typing import List

from .processor_state import (
    State,
    StateConfigLM,
    extract_values_from_query_state_by_key_definition
)

from .base_processor import BaseProcessor
from .processor_state_storage import StateMachineStorage

from .utils.general_utils import (
    build_template_text,
    calculate_string_dict_hash,
    clean_string_for_ddl_naming
)
logging = log.getLogger(__name__)


class BaseProcessorLM(BaseProcessor):

    @property
    def config(self) -> StateConfigLM:
        return self.output_state.config

    @property
    def user_template(self):
        if self.config.user_template_id:
            template = self.storage.fetch_template(self.config.user_template_id)
            return template.template_content
        return None

    @property
    def system_template(self):
        if self.config.system_template_id:
            template = self.storage.fetch_template(self.config.system_template_id)
            return template.template_content
        return None

    def __init__(self,
                 state_machine_storage: StateMachineStorage,
                 output_state: State,
                 processors: List[BaseProcessor] = None,
                 *args, **kwargs):
        super().__init__(
            output_state=output_state,
            processors=processors,
            **kwargs
        )

        # the storage engine
        self.storage = state_machine_storage

        # ensure that the configuration passed is of StateConfigLM
        if not isinstance(self.output_state.config, StateConfigLM):
            raise ValueError(f'invalid state config, '
                             f'got {type(self.output_state.config)}, '
                             f'expected {StateConfigLM}')

        logging.info(f'starting instruction state machine: {type(self)} with config {self.config}')

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    def apply_states(self, query_states: [dict]):
        logging.debug(f'persisting processed new query states from response. query states: {query_states} ')
        return [self.output_state.apply_query_state(query_state=qs) for qs in query_states]

    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        if not input_query_state:
            return

        # create the input query state entry primary key hash string
        input_query_state_key_hash, input_query_state_key_plain = (
            self.output_state.build_row_key_from_query_state(query_state=input_query_state)
        )

        # skip processing of this query state entry if the key exists, unless forced to process
        if self.has_query_state(query_state_key=input_query_state_key_hash, force=force):
            return

        # build final user and system prompts using the query state entry as the input data
        status, user_prompt = build_template_text(self.user_template, input_query_state)
        status, system_prompt = build_template_text(self.system_template, input_query_state)

        # begin the processing of the prompts
        try:

            # execute the underlying model function
            response_data, response_data_type, response_raw_data = (
                self._execute(user_prompt=user_prompt,
                              system_prompt=system_prompt,
                              values=input_query_state))

            logging.debug(f'processed prompt query: {input_query_state_key_plain} and received response: {response_data}')

            # we build a new output state to be appended to the output states
            output_query_state = {
                'state_key': input_query_state_key_hash,
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
                'response': response_data,
                'raw_response': response_raw_data if response_raw_data else '<<<blank>>>',
                'status': 'Success'
            }

            # fetch any additional state from the input states, provided that the config_name parameter is set
            # otherwise it will default to the only parameter it knows of named 'query' within the input states
            additional_query_state = extract_values_from_query_state_by_key_definition(
                key_definitions=self.config.query_state_inheritance,
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
                        'state_key': input_query_state_key_hash,
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

        except Exception as e:
            self.failed_process_query_state(e, query_state=input_query_state)

    def failed_process_query_state(self, e, query_state: dict):
        if isinstance(SyntaxError, e):
            logging.error(f'unable to parse response, skipping question {query_state} on {self.config}')
        elif isinstance(Exception, e):
            logging.error(f'critical error handling question {query_state} on {self.config} client gave up')
