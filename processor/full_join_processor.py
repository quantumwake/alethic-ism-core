import logging
import re
from typing import List

import utils
from processor.base_processor import BaseProcessor
from processor.processor_state import State


class FullJoinStateProcessor(BaseProcessor):

    def handle(self, processor: BaseProcessor, input_state: State):
        # TODO must return some sort of state update? or maybe not just needs a healthcheck
        processor(input_state=input_state)

    def __downstream(self, input_state: State, *args, **kwargs):
        if not self.processors:
            logging.warning(f'no downstream processors available to propagate input query state. '
                            f'current config processor: {self.config}')
            return

        for processor_node in self.processors:
            logging.debug(f'processing input state {input_state.config}')
            func = utils.higher_order_routine(self.handle,
                                              processor=processor_node,
                                              input_state=input_state)
            self.manager.add_to_queue(func)

        # wait for the completion of all processors or some exit code?
        # TODO should be running a health check on this
        self.manager.wait_for_completion()


    def __call__(self, states: List[State], *args, **kwargs):
        # TODO needs to be more flexible multiple input states
        source1 = states[0]
        source2 = states[1]

        def replace_variable(match, query_state: dict):
            variable_name = match.group(1)  # Extract variable name within {{...}}
            return query_state.get(variable_name, "{" + variable_name + "}")  # Replace or keep original

        # TODO need to fix this loading issue where default values are not being initalized
        src_count_1 = utils.implicit_count_with_force_count(state=source1)
        src_count_2 = utils.implicit_count_with_force_count(state=source2)

        logging.info(f'starting processing loop with size {src_count_1} for state config {source1.config}')

        # we need to generate the state keys
        for source_index_1 in range(src_count_1):
            query_state = source1.get_query_state_from_row_index(source_index_1)

            ## TODO temporarily
            query_template = query_state['query_template']
            # state_key = utils.build_state_data_row_key(query_state=query_state,
            #                                            key_definitions=source1.config.output_primary_key_definition)

            # TODO total hackjob (just a product of the two states + templating of values)

            # source.add_row_data_mapping(state_key=state_key, index=index)
            for source_index_2 in range(src_count_2):
                source_query_state_2 = source2.get_query_state_from_row_index(source_index_2)
                replace_variable_ho = utils.higher_order_routine_v2(replace_variable, query_state=source_query_state_2)
                completed_template = re.sub(r'\{(\w+)\}', replace_variable_ho, query_template)
                new_query_state = {'query': completed_template}
                joined_query_state = {**source_query_state_2, **new_query_state}
                self.save_state(query_state=joined_query_state)

        # execute downstream
        self.__downstream(input_state=self.state)


    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        # TODO handle this in a distributed way
        #  we could handle each individual state separately
        #  but for now we are simply passing the entire state as an input
        super().process_input_data_entry()
        pass
