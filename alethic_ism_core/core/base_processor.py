import logging as log

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


class BaseProcessor(Monitorable):

    def __init__(self,
                 output_state: State,
                 state_machine_storage: StateMachineStorage,
                 provider: ProcessorProvider = None,
                 processor: Processor = None,
                 output_processor_state: ProcessorState = None,
                 state_router_route: Route = None,
                 sync_store_route: Route = None,
                 **kwargs):

        super().__init__(**kwargs)

        # TODO move into a Syncable and StateRouteable feature class
        self.sync_store_route = sync_store_route
        self.state_router_route = state_router_route

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
        try:
            route_id = self.output_processor_state.id

            await self.send_processor_state_update(
                route_id=route_id,
                status=ProcessorStatusCode.RUNNING
            )

            output_query_states = await self.process_input_data_entry(
                input_query_state=input_query_state,
                force=force)

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
