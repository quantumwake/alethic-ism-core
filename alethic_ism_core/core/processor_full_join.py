import copy
import logging as log
from typing import List

from .processor_state import State, implicit_count_with_force_count
from .base_processor import BaseProcessor
from .utils.general_utils import higher_order_routine

logging = log.getLogger(__name__)


class DualStateProcessor(BaseProcessor):

    def __call__(self,
                 state_one: State,
                 state_two: State,
                 column_selector: List[str] = None,
                 *args, **kwargs):

        # TODO need to fix this loading issue where default values are not being initalized
        state_1_count = implicit_count_with_force_count(state=state_one)
        state_2_count = implicit_count_with_force_count(state=state_two)

        logging.info(f'starting full merge of state 1: {state_1_count}, and state 2: {state_2_count}')

        # we need to generate the state keys
        for index_1 in range(state_1_count):
            query_state_source_1 = state_one.build_query_state_from_row_data(index_1)

            for index_2 in range(state_2_count):
                query_state_source_2 = state_two.build_query_state_from_row_data(index_2)
                joined_query_state = {**query_state_source_1, **query_state_source_2}
                joined_query_state = self.apply_query_state(query_state=joined_query_state)
                print(joined_query_state)

        return self.output_state  # new state


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
            func = higher_order_routine(self.handle,
                                        processor=processor_node,
                                        input_state=input_state)

            self.manager.add_to_queue(func)

        # wait for the completion of all processors or some exit code?
        # TODO should be running a health check on this
        self.manager.wait_for_completion()

    def __call__(self, states: List[State],
                 column_selector: List[str] = None,
                 *args, **kwargs):

        state_count = len(states)
        state_one = states[0]

        # iterate through the list of states consecutively joining one state to the next
        for index in range(1, state_count):
            state_two = states[index]

            # logging.info(f'merging state one: {state.count} with state two: {state.count}')

            # fetch the state information before reseting it
            merger = DualStateProcessor(
                output_state=State(
                    config=copy.deepcopy(self.config)
                )
            )

            state_one = merger(state_one=state_one, state_two=state_two)


        # assign the final state to the current processors state
        self.state = state_one

        # execute downstream
        self.__downstream(input_state=self.state)


    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        # TODO handle this in a distributed way
        #  we could handle each individual state separately
        #  but for now we are simply passing the entire state as an input
        super().process_input_data_entry()
        pass
