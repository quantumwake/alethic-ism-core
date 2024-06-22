import logging as log
from typing import Callable, Dict

from .base_model import ProcessorState
from .processor_state import (
    StateConfigLM,
    extract_values_from_query_state_by_key_definition
)

from .base_processor import BaseProcessor
from .utils.general_utils import (
    build_template_text,
)

logging = log.getLogger(__name__)


class BaseProcessorLM(BaseProcessor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ensure that the configuration passed is of StateConfigLM
        if not isinstance(self.output_state.config, StateConfigLM):
            raise ValueError(f'invalid state config, '
                             f'got {type(self.output_state.config)}, '
                             f'expected {StateConfigLM}')

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    def apply_states(self, query_states: [dict]):
        logging.debug(f'persisting processed new query states from response. query states: {query_states} ')

        return [
            self.output_state.apply_query_state(
                query_state=query_state,
                scope_variable_mappings={
                    "provider": self.provider
                }
            )
            for query_state in query_states
        ]

    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        if not input_query_state:
            return

        # TODO maybe validate the input state to see if it was already processed for this particular output state?
        #
        # # create the input query state entry primary key hash string
        # input_query_state_key_hash, input_query_state_key_plain = (
        #   TODO this was the old way, needs to use the input state id's primary key not the output state's primary key.
        #       alternatively this should be handled at the state-router
        #   self.output_state.build_row_key_from_query_state(query_state=input_query_state)
        # )
        #
        # # skip processing of this query state entry if the key exists, unless forced to process
        # if self.has_query_state(query_state_key=input_query_state_key_hash, force=force):
        #     return

        # build final user and system prompts using the query state entry as the input data
        status, user_prompt = build_template_text(self.user_template, input_query_state)
        status, system_prompt = build_template_text(self.system_template, input_query_state)

        # begin the processing of the prompts
        try:

            # execute the underlying model function
            response_data, response_data_type, response_raw_data = (
                self._execute(
                    user_prompt=user_prompt,
                    system_prompt=system_prompt,
                    values=input_query_state
                )
            )

            # we build a new output state to be appended to the output states
            output_query_state = {
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
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
                output_query_state = {
                    **output_query_state,
                    **response_data,
                    # 'response': response_raw_data
                }

                query_states.append(output_query_state)
            elif isinstance(response_data, list):
                for item in list(response_data):
                    if not isinstance(item, dict):
                        raise ValueError(f'item in response list of rows must be in of type dict.')

                    # state_item_key = calculate_string_dict_hash(item)
                    output_query_state = {**{
                        # 'state_item_key': state_item_key,
                        'user_prompt': user_prompt,
                        'system_prompt': system_prompt,
                    }, **item, **additional_query_state}

                    # format column names and append the state key
                    query_states.append(output_query_state)
            else:
                output_query_state = {
                    **output_query_state,
                    'response': response_data
                }

                query_states.append(output_query_state)

            # write the query states (synchronized)
            query_states = self.apply_states(query_states=query_states)

            # return the updated query state after persistence, generally the same unless it was remapped to new columns
            return query_states

        except Exception as exception:
            await self.fail_execute_processor_state(
                # self.output_processor_state,
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_query_state
            )


    # def failed_process_query_state(self, exception: e, query_state: dict):
    #     if isinstance(SyntaxError, e):
    #         logging.error(f'unable to parse response, skipping question {query_state} on {self.config}')
    #     elif isinstance(Exception, e):
    #         logging.error(f'critical error handling question {query_state} on {self.config} client gave up')
