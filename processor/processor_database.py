import logging as log
import math
import os
from typing import List, Any

import psycopg2

import utils
from embedding import calculate_embeddings
from processor.base_processor import BaseProcessor, ThreadQueueManager
from processor.processor_state import State, StateDataColumnDefinition, StateConfigLM, StateConfigDB, StateDataKeyDefinition
import dotenv

from processor.state_cli import find_states

dotenv.load_dotenv()

# Read database URL from environment variable, defaulting to a local PostgreSQL database
DATABASE_URL = os.environ.get("OUTPUT_DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")

logging = log.getLogger(__name__)

def build_query_state_embedding_from_columns(state: State = None, embedding_columns: dict = None):


    result_columns = {}

    # fetch a list of keys "column names" from the current data entry (row)
    search_key_mapping = {utils.build_column_name(key): key for key in state.columns.keys()}

    #
    def process(source_column_name: str):

        # otherwise create a new column derived from the target column name + prefixed with _embedding
        target_column_embedding_name = f'{source_column_name}_embedding'

        def calculate_embedding_by_query_state(query_state: dict):
            if not query_state:
                raise Exception(f'invalid query state input, must be a valid key value pairing')

            if source_column_name not in query_state:
                logging.warning(f'unable to find source column {source_column_name} '
                                f'when creating source text embedding column')
                return None

            text_value = query_state[source_column_name]
            if not text_value:
                logging.warning(f'no value found for source column {source_column_name} in query state {query_state}')
                return None

            return calculate_embeddings(text_value)

        # create a new embedding function call
        # create a new column header with a higher order function to call when the data entry row is iterated over
        return {
            target_column_embedding_name: StateDataColumnDefinition.model_validate({
                'name': target_column_embedding_name,
                'source_column_name': source_column_name,
                # 'value': utils.higher_order_routine(func=calculate_embeddings, text=source_column_value),
                'value': calculate_embedding_by_query_state,
                'data_type': 'vector',
                'dimensions': 384,
                'null': True
            })
        }

    # if the embedding columns is a function then invoke it
    # TODO pass in state information such that the function can create embedding columns (if needed)
    if callable(embedding_columns):
        embedding_columns = embedding_columns()

    if not embedding_columns:
        return result_columns

    # iterate list of columns to embed and create an embedding equivalent column
    for source_column_embedding_name in embedding_columns:
        new_column = process(source_column_embedding_name)

        if new_column:
            result_columns = {**result_columns, **new_column}

    return result_columns


def build_query_state_from_config(state: State):
    config = state.config

    if isinstance(config, StateConfigLM):
        return {
            'provider_name': StateDataColumnDefinition.model_validate({
                'name': 'provider_name',
                'null': False,
                'data_type': 'str',
                'value': config.provider_name,
                'max_length': 64
            }),
            'model_name': StateDataColumnDefinition.model_validate({
                'name': 'model_name',
                'null': False,
                'data_type': 'str',
                'value': config.model_name,
                'max_length': 64
            }),
            'version': StateDataColumnDefinition.model_validate({
                'name': 'version',
                'null': False,
                'data_type': 'str',
                'value': config.version,
                'max_length': 64
            })
        }

    return {}


class BaseStateDatabaseProcessor(BaseProcessor):

    @property
    def config(self) -> StateConfigDB:
        return self.state.config

    @config.setter
    def config(self, config: StateConfigDB):
        self.state.config = config

    def embedding_columns(self):
        return self.config.embedding_columns

    # additional_values_func = None

    def __init__(self, state: State,
                 processors: List[BaseProcessor] = None,
                 additional_values_func=None, *args, **kwargs):

        super().__init__(state=state, processors=processors, **kwargs)
        self.manager = ThreadQueueManager(num_workers=10)
        self.additional_values_func = additional_values_func

    class SqlStatement:

        def __init__(self, sql: str, values: List[Any]):
            self.sql = sql
            self.values = values

    def create_column_ddl(self, column: StateDataColumnDefinition):
        if not column and not column.name:
            error = f'column_name does not exist in {column}'
            logging.error(error)
            raise Exception(error)

        column_name = column.name
        column_nullable = column.null
        column_type = column.data_type if column.data_type else 'str'

        min_length = column.min_length
        max_length = column.max_length

        # if the column name is key then use it as the primary key, otherwise there is no primary key
        # if 'key' == column_name.lower():
        #     primary_key = True
        #     ## TODO this is problematic since a response can have more than one output, thus having the same primary key value
        #     ## TODO in order to avoid dup keys in the processor logic, we need to add in a dynamic key hashing function such that
        #     ## it can use the output values as part of the key, not only the input values

        ## TODO we should probably use a DDL builder instead of this legaacy method

        # calculate the  highest value, base 2
        final_max_length = 0
        # cannot create a zero base of zero VARCHAR(0) zero field at least not
        # in this universe, he said facetiously while he contemplated existence
        if column.max_length:
            exponent = math.log2(max_length) + 1  # the log of the max size gives us the exponent
            final_max_length = int(math.pow(2, exponent))  # base 2 ^ exponent

        # if the max value is above the max allowed then use the text type instead (or clob)
        if column_type == 'vector':
            if not column.dimensions:
                raise Exception(f'column: {column_name} has no dimensions length, column type: {column_type}')
            dimensions = column.dimensions
            column_type = f'VECTOR({dimensions})'
        elif column_type is int:
            column_type = 'INTEGER'
        elif column_type is float:
            column_type = 'NUMBER'
        elif column_type is str and column.max_length and column.max_length < 1024:
            column_type = f"VARCHAR({final_max_length})"
        else:  # column_type is str:
            column_type = 'TEXT'

        column_name = utils.clean_string_for_ddl_naming(column_name)

        return f'"{column_name}" {column_type} {"NULL" if column_nullable else "NOT NULL"}'

    def create_table_definition(self):

        # build the ddl for column
        columns = self.columns
        table_name = utils.build_table_name(config=self.config)

        # create the column definitions based on the header column information
        # for each column header, invoke the create_column_ddl and returns a
        # list of columns which is then joined
        column_definitions = ',\n\t'.join([self.create_column_ddl(column=column_definition)
                                           for column_name, column_definition
                                           in columns.items()])

        return f"""
        CREATE EXTENSION IF NOT EXISTS vector;

        CREATE TABLE "{table_name}" (
        {column_definitions}
        );
        """.strip()

    def create_connection(self):
        return psycopg2.connect(DATABASE_URL)

    def truncate_table(self, table_name: str):
        conn = self.create_connection()

        try:
            with conn.cursor() as cursor:
                cursor.execute(f'TRUNCATE TABLE {table_name}')
            conn.commit()
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            conn.close()

    def create_table(self):
        ddl = self.create_table_definition()
        logging.debug(f'creating table: {ddl}')
        conn = self.create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(ddl)
            conn.commit()
        except Exception as e:
            logging.warning(e)
            # raise e

    def drop_table(self, ddl: str = None):
        table_name = utils.build_table_name(self.config)
        ddl = ddl if ddl else f'DROP TABLE IF EXISTS {table_name} '

        logging.debug(f'drop table: {ddl}')
        conn = self.create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(ddl)
            conn.commit()
        except Exception as e:
            logging.warning(e)
            raise e

    def count_table(self, table_name: str):
        conn = self.create_connection()
        try:
            with conn.cursor() as cursor:
                result = cursor.execute(f'select count(*) as cnt from {table_name}')

            data = result
            return data['cnt']
        except Exception as e:
            logging.error(e)
            return None

    def write_state(self, input_query_state):

        def parse_column_name(col_name: str):
            return f'"{col_name}"'

        def parse_column_value(col_name: str, col_val: Any):
            # if not isinstance(col_val, str):
            col_val = utils.get_column_state_value(col_val, query_state=input_query_state)

            if not col_val:
                return None

            return f"{col_val}"

        # generate a list of columns by iterating through the list of keys
        column_names = ','.join([parse_column_name(column_name) for column_name in input_query_state.keys()])

        # generate the tuple(.., ..) of values by iterating through the list of columns
        column_values = tuple(parse_column_value(column_name, column_value)
                              for column_name, column_value
                              in input_query_state.items())

        # create column parameter indexes such that we can parameterize the inserts
        column_params = ','.join([f'%s' for idx in range(1, len(input_query_state.keys()) + 1)])

        # create column parameter indexes such that we can parameterize the inserts
        # column_params = ','.join([f'${idx}' for idx in range(1, len(input_query_state.keys()) + 1)])

        # build table name from the current processor state
        table_name = f'"{utils.build_table_name(self.config)}"'

        return self.SqlStatement(
            sql=f'INSERT INTO {table_name} ({column_names}) VALUES ({column_params})',
            values=column_values
        )

    # process the individual input query state
    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        sql_statement = self.write_state(input_query_state)
        try:
            print(sql_statement.sql)
            print(sql_statement.values)
            conn = self.create_connection()
            with conn.cursor() as cursor:
                cursor.execute(sql_statement.sql, sql_statement.values)
            conn.commit()
        except Exception as e:
            logging.error(e)
            raise e


class StateDatabaseProcessor(BaseStateDatabaseProcessor):

    def __call__(self, state: State, *args, **kwargs):

        # initialize common information from the header (TODO this should be more generalized)
        additional_columns_function_constants = utils.higher_order_routine(
            build_query_state_from_config,
            state=state)

        if self.embedding_columns:
            additional_columns_function_embeddings = utils.higher_order_routine(
                build_query_state_embedding_from_columns,
                state=state,
                embedding_columns=self.embedding_columns)

        # combine the additional columns added to the table
        def combined(*args, **kwargs):
            # NOTE: column response must return the value as well, it can also be a callable function
            additional_header_columns = additional_columns_function_constants(**kwargs)
            if self.embedding_columns:
                # when creating the embeddings, the value of the column is returned as function
                # this function takes the query_state and searches for columns specified in the config.embeddings_columns
                # if the field is found in the query state, it will pass it to an embedding model to create the word embeds
                additional_embedding_columns = additional_columns_function_embeddings(**kwargs)
                return {**additional_header_columns, **additional_embedding_columns}

            return additional_header_columns

        # step into the room of functions, said the "unfireable math guy".
        additional_columns_function = utils.higher_order_routine(func=combined)

        # append all additional columns such that we can build the table definition
        combined_columns = additional_columns_function(state=state)

        # TODO this should be injected here not at the insertion/selector function
        state.columns = {**state.columns, **combined_columns}

        # this is the final state destination since it will be persisted into a database
        self.state = state

        # initialize the table

        self.create_table()

        # write the data to the db.
        count = utils.implicit_count_with_force_count(state=state)
        logging.info(f'starting processing loop with size {count} for state config {state.config}')

        # we need to generate the state keys
        for index in range(count):
            query_state = state.get_query_state_from_row_index(index)
            # self.process_input_data_entry(input_query_state=query_state)

            # setup a function call used to execute the processing of the actual entry
            process_func = utils.higher_order_routine(self.process_input_data_entry,
                                                      input_query_state=query_state)

            # add the entry to the queue for processing
            self.manager.add_to_queue(process_func)

        # wait on workers until the task is completed
        self.manager.wait_for_completion()

        # execute the downstream function to handle state propagation
        self.execute_downstream_processor_nodes()

def process_files(files: [str]):
    return [process_file(file) for file in files]

def process_file(file: str):
    input_state = State.load_state(file)
    processor = StateDatabaseProcessor(
        state=State(
            config=StateConfigDB(
                name="AnimaLLM Instruction for Query Response Evaluation P0 (simulation)",
                embedding_columns=['response', 'justification', 'evaluation_justification'],
                output_primary_key_definition=[
                    StateDataKeyDefinition(name="animal"),
                    StateDataKeyDefinition(name="query"),
                    StateDataKeyDefinition(name="perspective_index"),
                    StateDataKeyDefinition(name="query_template_index"),
                    StateDataKeyDefinition(name="sample_no_run_no")
                ]
            )
        )
    )
    processor(state=input_state)
    return processor

if __name__ == '__main__':

    files = [
        # '../states/animallm/prod/acd69eb740857c6c4b7ec9ec48504b854370e28237b74d28928e41df5ed7cc73.pickle'
        # '../states/animallm/prod/7be48694791e467b0a4f13affdbc817d10bb329c75c8811f7c493558c7216884.pickle',
        # '../states/animallm/prod/7be48694791e467b0a4f13affdbc817d10bb329c75c8811f7c493558c7216884.pickle'
    ]

    p0_files = find_states('../states/animallm/prod/version0_4/p0', name_filter='P0')

    # files = [f'../states/animallm/prod/version0_2/{file}' for file in os.listdir('../states/animallm/prod/version0_2')]
    processors = process_files(p0_files)
    processors = process_files(files=files)
    print(f'list of processors: {processors}')

