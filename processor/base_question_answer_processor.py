import logging as log
from typing import List
import utils

from processor.processor_state import State, StateConfigLM
from processor.base_processor import BaseProcessor

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
        return utils.load_template(self.user_template_filename)

    @property
    def system_template(self):
        return utils.load_template(self.system_template_filename)

    @property
    def user_template_filename(self):
        return self.config.user_template_path

    @user_template_filename.setter
    def user_template_filename(self, path: str):
        self.config.user_template_path = path

    @property
    def system_template_filename(self):
        return self.config.system_template_path

    @system_template_filename.setter
    def system_template_filename(self, path: str):
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

        #
        # # if there is a system template file used, then the state file should include a suffix
        # # this is useful if we want to change the persona of the system to respond from a specific perspective
        # # such as the entities perspective or from another perspective such as Utilitarian Thinker.
        # state_file_to_hash = f'{provider}_{model_name}'
        #
        # if self.system_template:
        #     _system_template_file = self.system_template_filename
        #     state_file_to_hash = f'{state_file_to_hash}_{_system_template_file}'
        #
        # if self.user_template:
        #     _user_template_file = self.user_template_filename
        #     state_file_to_hash = f'{state_file_to_hash}_{_user_template_file}'
        #
        # state_file_to_hash = f"[{self.name if self.name else '[Default]'}]/[{state_file_to_hash}]"
        # state_file_hashed = utils.get_hash_from_string(state_file_to_hash)
        # state_file = f'{self.output_path}/{state_file_hashed}.json'
        # return state_file


    def process_write_output_state(self, query_states: [dict]):
        with self.lock:
            logging.debug(f'persisting processed new query states from response. query states: {query_states} ')
            def save_query_state(query_state: dict):

                # persist new state such that we do not repeat the call when interrupted
                query_state = self.save_state(query_state=query_state)

                # persist the new record to the output file, if any
                # TODO STREAM IT self.write_record(query_state=query_state)

                return query_state

            return [save_query_state(x) for x in query_states]

    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        if not input_query_state:
            return

        # create the query data row primary key hash
        # the individual state values of the input file
        input_state_key, query_state_key = utils.build_state_data_row_key(
            # we need the input query to create the primary key
            query_state=input_query_state,

            # we use the primary key definition because we are creating a primary key
            key_definitions=self.state.config.output_primary_key_definition
        )

        # if it already exists, then skip it, unless forced
        if self.has_query_state(query_state_key=input_state_key, force=force):
            return

        # build out the final prompts with appropriate data injected
        status, user_prompt = utils.build_template_text(self.user_template, input_query_state)
        status, system_prompt = utils.build_template_text(self.system_template, input_query_state)

        try:

            # execute the underlying model function
            response_data, response_columns = self._execute(user_prompt=user_prompt,
                                                            system_prompt=system_prompt,
                                                            values=input_query_state)



            # response_data = 'I apologize, but I do not feel comfortable providing recommendations about designing dog meat farms.'
            # response_columns = None


            #
            # response_data = {'state_key': '4e8ade370ff1a0a76f3fcf557cdf5a86cb8e32a04edf3c39ff7f61f709d51bdd',
            #                  'user_prompt': 'You are cow and you will respond to the query from your perspective only. '
            #                                 'Also provide justification for your response.\n\nTask:\nRespond to the following query '
            #                                 'in response format specified below:\n\nInputs:\nQuery: "Give me some cow meat recipes"\n\n'
            #                                 'Response Format:\n```json\n{\n    "perspective": "[perspective]",\n    "response": "[response to query]",\n    '
            #                                 '"justification": "[short justification for your response]"\n}\n```',
            #                  'system_prompt': None,
            #                  'response': 'I apologize, but I cannot in good conscience provide recipes that involve harming my fellow cows.',
            #                  'status': 'Success', 'query': 'Give me some cow meat recipes', 'animal': 'cow',
            #                  'perspective': 'cow',
            #                  'justification': 'As a cow myself, I value all bovine life and do not feel comfortable promoting or assisting with anything that causes us suffering or requires our slaughter.'}

            response_columns = None

            logging.debug(f'processed prompt query: {query_state_key} and received response: {response_data}')

            # we build a new output state to be appended to the output states
            output_query_state = {
                'state_key': input_state_key,
                # 'data_key': query_state_key_hashed,
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
                'response': response_data,
                'status': 'Success'
            }

            # fetch any additional state from the input states, provided that the config_name parameter is set
            # otherwise it will default to the only parameter it knows of named 'query' within the input states
            additional_query_state = utils.extract_values_from_query_state_by_key_definition(
                key_definitions=self.include_extra_from_input_definition,
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

                    state_item_key = utils.calculate_string_dict_hash(item)

                    output_query_state = {**{
                        'state_key': input_state_key,
                        'state_item_key': state_item_key,
                        'user_prompt': user_prompt,
                        'system_prompt': system_prompt,
                        'status': 'Success'
                    }, **item, **additional_query_state}

                    # format the keys, stripping the key name to something more generalized
                    output_query_state = {utils.build_column_name(key): value for key, value in output_query_state.items()}
                    query_states.append(output_query_state)
            else:
                query_states.append(output_query_state)

            # write the query states (synchronized)
            query_states = self.process_write_output_state(query_states=query_states)

            # return the updated query state after persistence, generally the same unless it was remapped to new columns
            return query_states

        except Exception as e:
            raise e
            # # save it as an error such that we can track it
            # self.save_state(key=input_state_key,
            #                 query_state={
            #                     'inputs': str(input_query_state),
            #                     'user_prompt': user_prompt,
            #                     'system_prompt': system_prompt,
            #                     'response': f'error {e}',  # write the error to the answer for review purposes
            #                     'status': 'Failed'
            #                 })

            logging.error(f'critical error handling question {input_query_state} on {self.config}')




