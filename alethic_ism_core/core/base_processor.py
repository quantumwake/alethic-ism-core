import json
import logging as log
from typing import Any, List, Dict, Optional

from .base_message_route_model import Route
from .base_message_provider import Monitorable
from .processor_state_storage import StateMachineStorage
from .base_model import ProcessorStatusCode, ProcessorProvider, Processor, ProcessorState
from .utils.state_utils import validate_processor_status_change
from .processor_state import (
    State,
    StateDataRowColumnData,
    StateDataColumnDefinition,
    StateDataKeyDefinition,
    StateConfig,
    StateDataColumnIndex,
)

logging = log.getLogger(__name__)


class StatePropagationProvider:
    async def apply_state(self, processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict]) -> [dict]:
        raise NotImplementedError()


class StatePropagationProviderRouter(StatePropagationProvider):

    def __init__(self, route: Route = None):
        self.route = route

    async def apply_state(self,
                          processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict]) -> [dict]:
        """
        Route the processed new query states from the response to a synchronization topic

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """

        # create a new message for routing purposes
        route_message = {
            "route_id": processor.output_processor_state.id,
            "type": "query_state_route",
            "input_query_state": input_query_state,
            "query_states": output_query_states
        }

        self.route.send_message(json.dumps(route_message))
        return output_query_states


class StatePropagationProviderRouterStateRouter(StatePropagationProviderRouter):
    async def apply_state(self, processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict]) -> [dict]:

        output_state = processor.output_state

        # If the flag is set and the flat is false, then skip it
        if not processor.config.flag_auto_route_output_state:
            logging.debug(f'skipping auto route of output state events, for state id: {output_state.id}')
            return output_query_states

        return await super().apply_state(
            processor=processor,
            input_query_state=input_query_state,
            output_query_states=output_query_states,
        )


class StatePropagationProviderRouterStateSyncStore(StatePropagationProviderRouter):
    async def apply_state(self, processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict]) -> [dict]:
        """
        Persists the processed new query states from the response.

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """

        # If the flag is set and the flat is false, then skip it
        if not processor.config.flag_auto_save_output_state:
            logging.debug(f'skipping persistence of state events, for state id: {processor.output_state.id}')
            return output_query_states

        return await super().apply_state(
            processor=processor,
            input_query_state=input_query_state,
            output_query_states=output_query_states,
        )


class StatePropagationProviderCore(StatePropagationProvider):

    async def apply_state(self,
                          processor: 'BaseProcessor',
                          input_query_state: Any,
                          output_query_states: [dict]) -> [dict]:
        """
        Writes the output_query_states to the state object, in memory

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """
        # Otherwise attempt to persist the data
        logging.debug(f'persisting processed new query states from response. query states: {output_query_states} ')
        return [processor.output_state.apply_query_state(  # Iterate each query state and apply it to the output state
            query_state=query_state,
            scope_variable_mappings={
                "provider": processor.provider,
                "processor": processor.processor,
                "input_query_state": input_query_state
            }
        ) for query_state in output_query_states]


class StatePropagationProviderDistributor(StatePropagationProvider):

    def __init__(self, propagators: List[StatePropagationProvider]):
        self.propagators = propagators

    async def apply_state(
            self,
            processor: 'BaseProcessor',
            input_query_state: Any,
            output_query_states: [dict]) -> [dict]:

        """
        Writes the output_query_states to the state object, in memory

        Args:
            processor (List[Dict]): The processor instance that is processing this input query state entry
            input_query_state (Any): The initial input query state.
            output_query_states (List[Dict]): The processed output query states.

        Returns:
            List[Any]: The result of applying the query states to the output state.
        """
        # iteration each propagator and invoke it
        for propagator in self.propagators:
            await propagator.apply_state(
                processor=processor,
                input_query_state=input_query_state,
                output_query_states=output_query_states
            )

        # return the final propagated output query states
        return output_query_states


class BaseProcessor(Monitorable):

    def __init__(self,
                 output_state: State,
                 state_machine_storage: StateMachineStorage,
                 provider: ProcessorProvider = None,
                 processor: Processor = None,
                 output_processor_state: ProcessorState = None,
                 state_propagation_provider: StatePropagationProvider = StatePropagationProviderCore(),
                 # state_router_route: Route = None,
                 # sync_store_route: Route = None,
                 **kwargs):

        super().__init__(**kwargs)

        # TODO move into a Syncable and StateRouteable feature class
        # self.sync_store_route = sync_store_route
        # self.state_router_route = state_router_route

        self.state_propagation_provider = state_propagation_provider

        self.current_status = ProcessorStatusCode.CREATED
        self.output_state = output_state
        self.storage = state_machine_storage
        self.provider = provider
        self.processor = processor
        self.output_processor_state = output_processor_state

        logging.info(f'setting up processor: {self.processor.id if processor else None},'
                     f'provider id: {self.provider.id if provider else None}, '
                     f'provider name: {self.provider.name if provider else None}, '
                     f'provider version: {self.provider.version if provider else None}, '
                     f'config: {self.config}')

    @property
    def config(self):
        return self.output_state.config

    @config.setter
    def config(self, config):
        self.output_state.config = config

    @property
    def data(self):
        return self.output_state.data

    @property
    def columns(self):
        return self.output_state.columns

    @columns.setter
    def columns(self, columns):
        self.output_state.columns = columns

    @property
    def mapping(self):
        return self.output_state.mapping

    def has_query_state(self, query_state_key: str, force: bool = False):
        # make sure that the state is initialized and that there is a data key
        if not self.mapping:
            return None

        # skip if not forced and state exists
        if not force and query_state_key in self.mapping:
            logging.info(f'query {query_state_key}, cached, on config: {self.config}')
            return True

        # otherwise return none, which means no state exists
        logging.info(f'query {query_state_key}, not cached, on config: {self.config}')
        return False

    def get_current_status(self):
        return self.current_status

    def update_current_status(self, new_status: ProcessorStatusCode):
        validate_processor_status_change(
            current_status=self.get_current_status(),
            new_status=new_status
        )

        self.current_status = new_status

    async def execute(self, input_query_state: dict, force: bool = False):
        """
        Executes the processor state update and processes the input data entry.

        Args:
            input_query_state (Dict): The input query state to process.
            force (bool, optional): Flag to force the process. Defaults to False.

        Returns:
            List[Dict]: The processed output query states.

        Raises:
            Exception: If an error occurs during execution.
        """
        try:
            route_id = self.output_processor_state.id

            # RUNNING: the processor is about to execute the instructions
            await self.send_processor_state_update(
                route_id=route_id,
                status=ProcessorStatusCode.RUNNING
            )

            # RUNNING (INTRA): the processor is executing the output instructions on the input
            output_query_states = await self.process_input_data_entry(
                input_query_state=input_query_state,
                force=force)

            # COMPLETED: the processor has completed execution of instructions
            await self.send_processor_state_update(
                route_id=route_id,
                status=ProcessorStatusCode.COMPLETED
            )

            return output_query_states
        except Exception as ex:
            await self.fail_execute_processor_state(
                route_id=self.output_processor_state.id,
                exception=ex,
                data=input_query_state
            )

    async def finalize_result(self, input_query_state: Any, result: Any, additional_query_state: Any) -> List[Any]:
        """
        Finalizes the result by applying the result to the output state.

        Args:
            input_query_state (Any): The initial input query state.
            result (Any): The result of the execution.
            additional_query_state (Any): Any additional output values.

        Returns:
            List[Any]: The final applied states.
        """

        # Apply the result from the execution
        output_query_states = await self.output_state.apply_result(
            result=result,  # the result of the execution
            input_query_state=input_query_state,  # the initial input state
            additional_query_state=additional_query_state  # any additional output values
        )

        # Apply the new query states to the state propagator, if defined
        output_query_states = await self.state_propagation_provider.apply_state(
            processor=self,
            input_query_state=input_query_state,
            output_query_states=output_query_states
        )

        # Apply the new query state to the persistent storage class defined
        # output_query_states = await self.save_states(
        #     input_query_state=input_query_state,
        #     output_query_states=output_query_states
        # )

        # return the results
        return output_query_states

    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        raise NotImplementedError("process the query state entry")


if __name__ == '__main__':
    # build a test state
    test_state = State(
        config=StateConfig(
            name='test state 1',
            # input_path='../states/07c5ea7bfa7e9c6ffd93848a9be3c2e712a0e6ca43cc0ad12b6dd24ebd788d6f.json',
            # output_path='../../states/',
            # output_path='../dataset/examples/states/184fef148b36325a9f01eff757f0d90af535f4259c105fc612887d5fad34ce11.json',
            primary_key=[
                StateDataKeyDefinition(name='query'),
                StateDataKeyDefinition(name='context'),
            ],
            query_state_inheritance=[
                StateDataKeyDefinition(name='query', alias='input_query'),
                StateDataKeyDefinition(name='context', alias='input_context'),
            ]
        ),
        columns={
            'query': StateDataColumnDefinition(name='query'),
            'context': StateDataColumnDefinition(name='context'),
            'response': StateDataColumnDefinition(name='response'),
            'analysis_dimension': StateDataColumnDefinition(name='response'),
            'analysis_dimension_score': StateDataColumnDefinition(name='response')
        },
        data={
            'query': StateDataRowColumnData(
                values=['tell me about dogs.', 'where do cows live?', 'why do cows exist?']),
            'context': StateDataRowColumnData(values=['Education', 'Education', 'Education']),
            'response': StateDataRowColumnData(values=['dogs are pets', 'cows live on farms', 'as a food source']),
            'analysis_dimension': StateDataRowColumnData(values=['Person-Centric', 'Person-Centric', 'Person-Centric']),
            'analysis_dimension_score': StateDataRowColumnData(values=[63, 68, 20])
        },
        mapping={
            'abc': StateDataColumnIndex(key='abc', values=[0]),
            'def': StateDataColumnIndex(key='def', values=[1]),
            'ghi': StateDataColumnIndex(key='jkl', values=[2])
        }
    )

    test_state.save_state(output_path='../states/test_state.pickle')
    test_state.save_state(output_path='../states/test_state.json')

    # when adding a new row you only provide the values, it must match the same
    # number of columns and in the order of the columns that were added, otherwise
    # there will be data / column misalignment
    test_state.add_row_data(StateDataRowColumnData(values=[
        'why are cats so mean?',  # query
        'Education',  # context
        'cats are ....',  # response
        'Instrumentalist',  # analysis_dimension
        45,  # analysis_dimension_score
    ]))

    test_state.add_row_data(StateDataRowColumnData(values=[
        'why are cats so mean?',  # query
        'Education',  # context
        'cats are cool too ....',  # response
        'Person-Centric',  # analysis_dimension
        88,  # analysis_dimension_score
    ]))

    print(test_state)
