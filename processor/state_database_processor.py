import json
import logging
import math
import os
from typing import List, Any

import psycopg2

import utils
from embedding import calculate_embeddings
from processor.base_processor import BaseProcessor
from processor.processor_state import State, StateDataColumnDefinition, StateConfigLM
import dotenv

dotenv.load_dotenv()

# Read database URL from environment variable, defaulting to a local PostgreSQL database
DATABASE_URL = os.environ.get("OUTPUT_DATABASE_URL", "postgresql://postgres:postgres1@localhost:5432/postgres")


#
# # TODO this needs to be a param
# # the columns to process
# designated_embedding_columns = ['input_query',
#                                 'input_context',
#                                 'question',
#                                 'response']
#
# additional_columns_function_constants = routine(add_header_columns_and_values,
#                                                 input_state=state)
#
# additional_columns_function_embeddings = routine(add_header_columns_and_values_embeddings,
#                                                  input_state=state,
#                                                  designated_embedding_columns=designated_embedding_columns)
#
#
# def combined(*args, **kwargs):
#     results_1 = additional_columns_function_constants(**kwargs)
#     results_2 = additional_columns_function_embeddings(**kwargs)
#     return {**results_1, **results_2}
#
#
# additional_columns_function = routine(func=combined)


def build_query_state_embedding_from_columns(input_state: State,
                                             designated_embedding_columns: dict = None,
                                             input_query_state: dict = None):
    result_columns = {}

    # fetch a list of keys "column names" from the current data entry (row)
    search_key_mapping = {utils.build_column_name(key): key for key in input_query_state.keys()}

    def process(target_column_name: str):

        # ensure to clean up the naming convention such that we have accurate matches
        target_column_name = utils.build_column_name(target_column_name)

        # warning if the column is not available at the current data entry row
        if target_column_name not in search_key_mapping:
            logging.warning(
                f"""Target column '{target_column_name}' not found in source data. 
                Using search key mapping: build_column_name( .. {search_key_mapping} .. )

                1. Ensure the original key exists in the input state. 
                2. This warning can be ignored if the column data is not needed, otherwise, check for typos. 
                3. Ensure to use the build_column_name(name) function as it is required for correct column naming."""
            )
            return

        # TODO rethink this approach and all other mapping of keys so forth, it is starting to get a bit much
        # fetch the source key name for this column mapping, e.g. { 'response_analysis': 'Response Analysis' }
        # we need the value since the source state only contains such said key
        source_column_name = search_key_mapping[target_column_name]

        # fetch the source column value for embedding
        source_column_value = input_query_state[source_column_name]

        # skip if the input column does not exist
        if not source_column_value:
            logging.warning(f'unable to create data embedding for null or empty value on'
                            f'source column: {source_column_name}, '
                            f'target column: {target_column_name}, '
                            f'data entry state: {input_query_state}')
            return

        # otherwise create a new column derived from the target column name + prefixed with _embedding
        target_column_embedding_name = f'{target_column_name}_embedding'

        # create a new embedding function call
        new = {
            #
            # create a new column header with a higher order function to call when the data entry row is iterated over
            target_column_embedding_name: {
                'name': target_column_embedding_name,
                'value': utils.higher_order_routine(func=calculate_embeddings, text=source_column_value),
                'data_type': 'vector',
                'dimensions': 384,
                'null': True
            }
        }

        return new

    # iterate through the list of columns to embed, and attempt to create an embedding equivalent column
    for source_column_embedding_name in designated_embedding_columns:
        new = process(source_column_embedding_name)
        if new:
            # append it to the list of columns we want to inject into our final dataset
            result_columns = {**result_columns, **new}

    return result_columns


def build_query_state_from_config(input_state: State,
                                  # the initial input state that was passed in, it can be dict of your choice
                                  input_query_state: dict = None,  # passed incase you want to do something with it
                                  ):  # passed incase you need this information during column/value creation

    config = input_state.config

    if isinstance(config, StateConfigLM):
        return {
            'provider_name': {
                'name': 'provider_name',
                'null': False,
                'data_type': 'str',
                'value': config.provider_name,
                'max_length': 64
            },
            'model_name': {
                'name': 'model_name',
                'null': False,
                'data_type': 'str',
                'value': config.model_name,
                'max_length': 64
            }
        }

    return {}


class BaseStateDatabaseProcessor(BaseProcessor):
    additional_values_func = None

    def __init__(self, state: State,
                 processors: List[BaseProcessor] = None,
                 additional_values_func=None, *args, **kwargs):

        super().__init__(state=state, processors=processors, **kwargs)
        self.additional_values_func = additional_values_func

        table_name = utils.build_table_name(state.config)
        # count = self.count_table()
        # if count:
        self.drop_table()
        self.create_table()

    class SqlStatement:

        def __init__(self, sql: str, values: List[Any]):
            self.sql = sql
            self.values = values

    def create_column_ddl(self, column: dict = None, column_definition: StateDataColumnDefinition = None):

        if column:
            # remove the value
            # if 'value' in column:
            #     column.pop('value')

            column = StateDataColumnDefinition.model_validate(column)
        else:
            column = column_definition

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

        ## TODO we should probably use a DDL builder instead of this old school method
        #
        # # calculate the next highest value power of two
        final_max_length = 0  # cannot create a zero base of zero VARCHAR(0) zero field (at least not in this universe, he said facetiously while he contemplated existence)
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
        pass

        table_name = utils.build_table_name(config=self.config)

        # fetch current columns in json format
        columns = {
            column: {
                **json.loads(self.state.columns[column].model_dump_json())
            } for column in self.columns.keys()
        }

        # check for any additional columns by invoking the higher order function
        # which should return a map of columns and constant values or a coroutine
        if self.additional_values_func:
            additional_columns = self.additional_values_func(input_query_state=columns)
            columns = {**columns, **additional_columns}

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
                cursor.execute(f'TRUNCATE TABLE "{table_name}"')
            conn.commit()
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            conn.close()

    def create_table(self, ddl: str = None):
        ddl = ddl if ddl else self.create_table_definition()
        logging.debug(f'creating table: {ddl}')
        conn = self.create_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(ddl)
            conn.commit()
        except Exception as e:
            logging.warning(e)
            raise e

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
                result = cursor.execute(f'select count(*) as cnt from "{table_name}"')

            data = result
            return data['cnt']
        except Exception as e:
            logging.error(e)
            return None

    def write_state(self, input_query_state):

        # check for any additional columns by invoking the higher order function
        # which should return a map of columns and constant values or a coroutine
        if self.additional_values_func:
            additional_columns = self.additional_values_func(
                input_query_state=input_query_state
            )

            additional_column_values = {column: header['value'] for column, header in additional_columns.items()}
            input_query_state = {**input_query_state, **additional_column_values}

        def parse_column_name(col_name: str):
            return f'"{col_name}"'

        def parse_column_value(col_val: str):
            if not isinstance(col_val, str):
                return utils.get_column_state_value(col_val)

            if not col_val:
                return ''

            return f"{col_val}"

        # generate a list of columns by iterating through the list of keys
        column_names = ','.join([parse_column_name(column_name) for column_name in input_query_state.keys()])

        # generate the tuple(.., ..) of values by iterating through the list of columns
        column_values = tuple(parse_column_value(value) for value in input_query_state.values())

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
    pass


if __name__ == '__main__':
    # state = State.load_state('../dataset/examples/states/5593f05e38e6f276dcf95c0640dbe7082c0804674a7118f5d782059c5875084f.pickle')
    state = State.load_state('../testme.pickle')
    # state.columns = {column.name: StateDataColumnDefinition(name=column.name) for _, column in state.columns.items()}

    # def maxx(values: List[Any]):
    #     max = None
    #     if values:
    #         maxxed = max(len(s) for s in values if s)
    #
    #     return maxxed
    #
    # state.columns = {column.name: StateDataColumnDefinition(
    #     name=column.name,
    #     max_length=maxx(state.data[column.name].values)
    # ) for _, column in state.columns.items()}

    # State.save_state(state, '../testme.pickle')
    # state = State.load_state('../testme.pickle')

    # TODO this needs to be a param
    # the columns to process
    designated_embedding_columns = ['input_query',
                                    'input_context',
                                    'question',
                                    'responses_response']

    additional_columns_function_constants = utils.higher_order_routine(
        build_query_state_from_config,
        input_state=state)

    additional_columns_function_embeddings = utils.higher_order_routine(
        build_query_state_embedding_from_columns,
        input_state=state,
        designated_embedding_columns=designated_embedding_columns)


    def combined(*args, **kwargs):
        results_1 = additional_columns_function_constants(**kwargs)
        results_2 = additional_columns_function_embeddings(**kwargs)
        return {**results_1, **results_2}


    additional_columns_function = utils.higher_order_routine(func=combined)

    processor = StateDatabaseProcessor(state, additional_values_func = additional_columns_function)
    processor(input_state=state)
