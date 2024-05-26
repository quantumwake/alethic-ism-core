import logging as log

from .processor_state_storage import StateMachineStorage
from .base_model import StatusCode, ProcessorProvider
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


class BaseProcessor:

    def __init__(self,
                 output_state: State,
                 state_machine_storage: StateMachineStorage,
                 provider: ProcessorProvider,
                 **kwargs):

        self.current_status = StatusCode.CREATED
        self.output_state = output_state
        self.storage = state_machine_storage
        self.provider = provider

        logging.info(f'setting up processor with provider: {self.provider.name}, '
                     f'version: {self.provider.version}, '
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

    #
    # def process_by_query_states(self, query_states: List[dict]):
    #
    #     if not query_states:
    #         error = f'*******INVALID INPUT QUERY STATE *********'
    #         logging.error(error)
    #         raise Exception(error)
    #
    #     self.update_current_status(StatusCode.RUNNING)
    #
    #     # iterate query_states and add them to the worker queue
    #     for query_state in query_states:
    #         # setup a function call used to execute the processing of the actual entry
    #         process_func = higher_order_routine(self.process_input_data_entry,
    #                                             input_query_state=query_state)
    #
    #         # add the entry to the queue for processing
    #         self.manager.add_to_queue(process_func)
    #
    #     # wait on workers until the task is completed
    #     self.manager.wait_for_completion()
    #
    #     # if the termination flag is set then log it
    #     if self.get_current_status() == StatusCode.TERMINATED:
    #         logging.warning(f'terminated processor: {self.config}, termination flag was set')
    #         return
    #     else:
    #         self.update_current_status(StatusCode.COMPLETED)
    #
    #     # execute the downstream function to handle state propagation
    #     # self.execute_downstream_processor_nodes()

    def get_current_status(self):
        return self.current_status

    def update_current_status(self, new_status: StatusCode):
        validate_processor_status_change(
            current_status=self.get_current_status(),
            new_status=new_status
        )

        self.current_status = new_status

    #
    # def __call__(self,
    #              # input_file: str = None,
    #              input_state: State = None,
    #              force_state_overwrite: bool = False,
    #              *args, **kwargs):
    #
    #     # only if the input state has data do we iterate the content
    #     if input_state and input_state.data:
    #         # we pass the input state otherwise we get the self.state count
    #         count = implicit_count_with_force_count(state=input_state)
    #         logging.info(f'starting processing loop with size {count} for state config {input_state.config}')
    #
    #         # initialize a thread pool
    #         logging.info(f'about to start iterating individual input states '
    #                      f'(aka input_query_state, essentially a single record used to as '
    #                      f'part of the template injection')
    #
    #         # update current status
    #         self.update_current_status(StatusCode.RUNNING)
    #
    #         # iterate through the list of queries to be made and add them to a worker queue
    #         for index in range(count):
    #             logging.info(f'processing query state index {index} from {count}')
    #
    #             # get the query_state for the current execution call
    #             query_state = input_state.build_query_state_from_row_data(index=index)
    #
    #             # setup a function call used to execute the processing of the actual entry
    #             process_func = higher_order_routine(self._process_input_data_entry,
    #                                                 input_query_state=query_state)
    #
    #             # add the entry to the queue for processing
    #             self.manager.add_to_queue(process_func)
    #
    #         # start the thread runner only when all the data has been added to the queue
    #         self.manager.start()
    #
    #         # wait on workers until the task is completed
    #         self.manager.wait_for_completion()
    #
    #         # if the termination flag is set then log it
    #         if self.get_current_status() == StatusCode.TERMINATED:
    #             logging.warning(f'terminated processor: {self.config}, termination flag was set')
    #             return
    #         else:
    #             self.update_current_status(StatusCode.COMPLETED)
    #
    #         # execute the downstream function to handle state propagation
    #         # self.execute_downstream_processor_nodes()
    #
    #     else:
    #         error = f'*******INVALID INPUT STATE or INPUT STATE FILE or STREAM*********\n' \
    #                 f'input_state: {input_state if input_state else "<not loaded>"}, \n' \
    #                 f'and or data: {input_state.data if input_state.data else "<not loaded>"}. \n' \
    #                 f'use one of the processor execution parameters, such as input_state=..'
    #
    #         logging.error(error)
    #         raise Exception(error)

    # def store_state(self, output_state_path: str):
    #
    # # persist the entire output state to the storage class
    # # fetch the state file name previously configured, or autogenerated
    # output_state_path = output_state_path if output_state_path else self.build_state_storage_path()
    # self.output_state.save_state(output_state_path)

    # def build_state_storage_path(self, output_extension: str = 'pickle', prefix: str = None):
    #     if not self.name:
    #         raise Exception(
    #             f'Processor name not defined, please ensure to define a '
    #             f'unique processor name as part, otherwise your states might '
    #             f'get overwritten or worse, merged.')
    #
    #     if has_extension(self.output_path, ['pkl', 'pickle', 'json', 'csv', 'xlsx']):
    #         return self.output_path
    #
    #     # create temporary state storage area : if the output path is not set and does not exists already
    #     if not self.output_path:
    #         self.output_path = DEFAULT_OUTPUT_PATH
    #         if not os.path.exists(self.output_path):
    #             os.mkdir(self.output_path)
    #
    #     # when directory, then simply prefix output path to config.name
    #     if os.path.isdir(self.output_path):
    #         to_be_hashed = self.name
    #         if prefix:
    #             to_be_hashed = f'[{prefix}]/[{to_be_hashed}]'
    #
    #         state_file_hashed = calculate_uuid_based_from_string_with_sha256_seed(to_be_hashed)
    #         state_file = f'{self.output_path}/{state_file_hashed}.{output_extension}'
    #         self.output_path = state_file
    #         return state_file
    #
    #     # otherwise return the full path
    #     return self.output_path
    #
    # def _process_input_data_entry(self, input_query_state: dict, force: bool = False):
    #     current_status = self.get_current_status()
    #     if current_status != StatusCode.RUNNING:
    #         self.manager.stop_all_workers()
    #         return False
    #
    #     # execute the query state
    #     return self.process_input_data_entry(
    #         input_query_state=input_query_state,
    #         force=force
    #     )

    #
    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        raise NotImplementedError("process the query state entry")

#
# def initialize_processors_with_same_state_config(config: StateConfig,
#                                                  processor_types: List[BaseProcessor]) -> List[BaseProcessor]:
#     if not processor_types:
#         raise Exception(f'no processor types specified')
#
#     for processor_index, processor in enumerate(processor_types):
#         if not isinstance(processor, type(BaseProcessor)):
#             error = f'expected processor inherited from {type(BaseProcessor)}, got {type(processor)}'
#             logging.error(error)
#             raise Exception(error)
#
#         logging.info(f'created processor type {processor} with state config {config}')
#         copy_config = copy.deepcopy(config)
#         processor_types[processor_index] = processor(state=State(config=copy_config))
#
#     return processor_types


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
