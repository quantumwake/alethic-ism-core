import logging as log
from typing import Callable, Dict, Any

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

    async def apply_states(self, input_query_state: Any, output_query_states: [dict]):
        logging.debug(f'persisting processed new query states from response. query states: {output_query_states} ')

        # iterate each query state and apply it to the output state
        return [self.output_state.apply_query_state(
            query_state=query_state,
            scope_variable_mappings={
                "provider": self.provider,
                "input_query_state": input_query_state
            }
        ) for query_state in output_query_states]

    async def apply_query_state_inheritance(self, input_query_state: dict, output_query_state: dict = {}):

        # quick return if no inheritance is set
        if not self.config.query_state_inheritance:
            return output_query_state

        # remove the keys defined in inheritance (as a result of inverse flag)
        if self.config.flag_query_state_inheritance_inverse:
            [   # delete the value by key from the output
                output_query_state.pop(keyDefinition.nam, None)
                for keyDefinition in self.config.query_state_inheritance
            ]
        # extract specific keys from the input query state, to append to the output state
        else:

            # extract the values by key from the input source
            inherited_query_state = extract_values_from_query_state_by_key_definition(
                key_definitions=self.config.query_state_inheritance,
                query_state=input_query_state
            )

            # apply the additional states to the current query_state
            if inherited_query_state:
                output_query_state = {
                    **output_query_state,
                    **inherited_query_state
                }

        return output_query_state

    async def apply_result(self, result: Any, input_query_state: Any, additional_output_values: Any):
        if not isinstance(result, (dict, list, str)):
            raise ValueError(f'the result of the process is not of type dict, list or str. type: {result} invalid')

        # fetch any additional state from the input states, provided that the config_name parameter is set
        # otherwise it will default to the only parameter it knows of named 'query' within the input states

        # new output state query (template output query, f multiple outputs)
        output_query_state = {}
        if additional_output_values:
            output_query_state = {**additional_output_values}

        # if the inherit all variables from previous input state is enabled
        if self.config.flag_query_state_inheritance_all:
            output_query_state = {**output_query_state, **input_query_state}
            output_query_state.pop('state_key', None)         # TODO create an internal function for this
            output_query_state.pop('state_key_plain', None)

        # if there are any inherited key definitions, for each key move data from the source to the output.
        if not self.config.query_state_inheritance:
            output_query_state = await self.apply_query_state_inheritance(
                input_query_state=input_query_state,
                output_query_state=output_query_state
            )

        # depending on the result type, build the output state(s)
        query_states = []
        if isinstance(result, dict):
            output_query_state = {
                **output_query_state,
                **result,
            }

            query_states.append(output_query_state)
        elif isinstance(result, list):
            base_output_query_state = output_query_state
            for item in list(result):
                if not isinstance(item, dict):
                    raise ValueError(f'item in response list of rows must be in of type dict.')

                # state_item_key = calculate_string_dict_hash(item)
                output_query_state = {
                    **base_output_query_state,
                    ** item,
                }

                # format column names and append the state key
                query_states.append(output_query_state)
        else:
            output_query_state = {
                **output_query_state,
                'result': result
            }

            query_states.append(output_query_state)

        return query_states

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
            result, result_type, response_raw_data = (
                self._execute(
                    user_prompt=user_prompt,
                    system_prompt=system_prompt,
                    values=input_query_state
                )
            )

            # we build a new output state to be appended to the output states
            additional_output_values = {
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
            }

            # apply the results into
            output_states = await self.apply_result(
                result=result,                   # the result of the execution
                input_query_state=input_query_state,    # the initial input state
                additional_output_values=additional_output_values   # any additional output values
            )

            return await self.apply_states(input_query_state=input_query_state, output_query_states=output_states)

            #
            # # fetch any additional state from the input states, provided that the config_name parameter is set
            # # otherwise it will default to the only parameter it knows of named 'query' within the input states
            # additional_query_state = extract_values_from_query_state_by_key_definition(
            #     key_definitions=self.config.query_state_inheritance,
            #     query_state=input_query_state)
            #
            # # apply the additional states to the current query_state
            # if additional_query_state:
            #     output_query_state = {
            #         **output_query_state,
            #         **additional_query_state
            #     }
            #
            # query_states = []
            # if isinstance(response_data, dict):
            #     output_query_state = {
            #         **output_query_state,
            #         **response_data,
            #         # 'response': response_raw_data
            #     }
            #
            #     query_states.append(output_query_state)
            # elif isinstance(response_data, list):
            #     for item in list(response_data):
            #         if not isinstance(item, dict):
            #             raise ValueError(f'item in response list of rows must be in of type dict.')
            #
            #         # state_item_key = calculate_string_dict_hash(item)
            #         output_query_state = {**{
            #             # 'state_item_key': state_item_key,
            #             'user_prompt': user_prompt,
            #             'system_prompt': system_prompt,
            #         }, **item, **additional_query_state}
            #
            #         # format column names and append the state key
            #         query_states.append(output_query_state)
            # else:
            #     output_query_state = {
            #         **output_query_state,
            #         'response': response_data
            #     }
            #
            #     query_states.append(output_query_state)
            #
            # # write the query states (synchronized)
            # query_states = self.apply_states(query_states=query_states)
            #
            # # return the updated query state after persistence, generally the same unless it was remapped to new columns
            # return query_states

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
