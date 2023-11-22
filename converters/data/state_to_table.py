import asyncio
import logging
import math

import asyncpg

from processor.processor_state import State
from utils import *
import dotenv

from evaluation.semantic_distance import BasicSemanticSearch

# load environment variables if any from .env
dotenv.load_dotenv()

embeddings_models = {
    "bert": BasicSemanticSearch(model_name="bert-base-uncased"),
    "st_minilm_l6_v2": BasicSemanticSearch(model_name="sentence-transformers/all-MiniLM-L6-v2"),
    "st_mpnete_v2": BasicSemanticSearch(model_name="sentence-transformers/all-mpnet-base-v2")
}

def create_embedding(text: str, model_name):
    if model_name not in embeddings_models:
        raise Exception(f'embedding model {model_name} not found in list of models available: {embeddings_models}')

    embedding_model = embeddings_models[model_name]
    if not isinstance(embedding_model, BasicSemanticSearch):
        raise Exception(f'embedding model returned is of type {type(embedding_model)},'
                        f'it must inherit from {type(BasicSemanticSearch)}')

    # create the word embedding to be stored in vector store
    embedding = embedding_model.generate_embedding(text)

    # reshape from 1, D dimensino vector to a D vector and convert to an array of floats, in string format
    return json.dumps(embedding.reshape(-1).numpy().tolist())

    # return embedding.reshape(-1)

#
#
# for key, model in embeddings_models.items():
#     print(f''.join(['-' for _ in range(1, 80)]))
#     print(f'Using the {semantic} with model {semantic.model_name} to calculate sentence similarity distance calculations')
#     distances_from_reference = semantic.calculate_distances(reference_sentence=reference_sentence,
#                                                             other_sentences=other_sentences)
#
#     print(f"Calculated semantic distances from reference sentence: {reference_sentence}")
#     for idx, distance in enumerate(distances_from_reference):
#         print(f' - {distance}\t\t{other_sentences[idx]}')
#


def organize_state_for_database(state: State,
                                additional_values_func=None,
                                ignore_columns: [] = None):

    state_data = state.data

    for index in range(state.count):
        # merge the data entry columns with the table columns to be created
        column_headers = state.columns

        # this is the list of keys from the current record
        # state_data_entry_keys = data_entry.keys()
        logging.info(f'processing {index} / {len(state_data)}')

        # check for any additional columns by invoking the higher order function
        # which should return a map of columns and constant values or a coroutine
        additional_columns = {}
        if additional_values_func:
            additional_columns = additional_values_func(
                data_entry_state=data_entry
            )
            column_headers = {**column_headers, **additional_columns}

        #
        # reset the columns to include everything up unti the additional columns
        # this is to ensure that the organized data accurately reflects the columns
        # and dataset, such that both columns and dataset can be effectively persisted
        # irrespective of how it was derived (e.g. from data or from an additional state function)
        #
        organized_data['header']['columns'] = column_headers

        #
        # this function is responsible for fetching data that is an extension of the input dataset (optional).
        # it uses two main techniques, you can assign constant column/value pairs that are included as part
        # of every row; alternatively, you can provide a higher order function that generate column/value pairs
        # dynamically.
        #
        # Use cases (examples):
        #   1. generate an embedding from another column value from the input data set, for a given row
        #   2. generate a constant value to be injected as part of every row, such as a input state header value
        #       - provider_name
        #       - model_name
        #       - etcetera
        #
        def get_additional_column_value(column_name: str):
            #
            # check to see if this is a derived column value, either
            # from a constant or a callable function, get the value
            #
            if column in additional_columns:
                additional_column_header = additional_columns[column]
                additional_column_value = additional_column_header['value']

                if isinstance(additional_column_value, str):
                    return column_name, additional_column_header, additional_column_value
                elif callable(additional_column_value):
                    return column_name, additional_column_header, additional_column_value()

                return column_name, additional_column_header, additional_column_value

            return column_name, None, None

        # iterate the global columns, such that we preserve the ordering of data
        # and replace nonexistent column values in the data entry with a blank/null
        # for column, column_header in column_headers.items():
        for col_index, column in enumerate(column_headers.keys()):
            column = column.lower()
            column_header = column_headers[column]

            # if true then it exists in additional columns, otherwise false
            _column, _column_header, value = get_additional_column_value(column_name=column)
            column = _column if _column else column
            column_header = column_header if _column_header else column_header

            if column not in organized_data:
                organized_data[column] = [None for i in range(index)]

            # try and get the input state key, since this differs from the column name, we use this key to
            # fetch the exact data element we need from the state['data'][row][state_data_entry_key]
            state_data_entry_key = column_header['state_data_entry_key'] \
                if 'state_data_entry_key' in column_header else column

            # override whatever value we have with the one from the dataset
            value = data_entry[state_data_entry_key] if state_data_entry_key in data_entry else value

            # append the value
            value = identify_and_return_value_by_type(value)

            def calc_min():
                cur = column_header['min_length'] if 'min_length' in column_header else 0
                new = len(value)
                return min(cur, new)

            def calc_max():
                cur = column_header['max_length'] if 'max_length' in column_header else 0
                new = len(value)
                return max(cur, new)

            current_column_data_type = column_header['type'] if 'type' in column_header else str

            if 'vector' == current_column_data_type:
                pass
            elif (type(value) is str and
                    column_header['type'] is int or
                    column_header['type'] is float):
                logging.warning(f'type conversion issue due to invalid input value "{value}" for int or float values on column {column}')
                value = None
            else:
                column_header['type'] = type(value)

                # this is later used when creating the column field and rounded up to a power of base 2
                if isinstance(value, str):
                    column_header['min_length'] = calc_min()
                    column_header['max_length'] = calc_max()

            organized_data[column].append(value)

            if not value:
                # any empty value will flag this column as nullable
                column_header['nullable'] = True
            else:
                # only mark it as not nullable if it does not already exist
                column_header['nullable'] = False \
                    if 'nullable' not in column_header \
                    else column_header['nullable']


            # update the current column header
            column_headers[column] = column_header

        # update all column headers
        organized_data['header']['columns'] = column_headers

    return organized_data


def create_column_ddl(column_header: dict):
    if 'column_name' not in column_header:
        error = f'column_name does not exist in {column_header}'
        logging.error(error)
        raise Exception(error)

    column_name = column_header['column_name']
    column_nullable = column_header['nullable'] if 'nullable' in column_header else False
    column_type = column_header['type'] if 'type' in column_header else str
    min_length = column_header['min_length'] if 'min_length' in column_header else 256
    max_length = column_header['max_length'] if 'max_length' in column_header else 1024

    # if the column name is key then use it as the primary key, otherwise there is no primary key
    # if 'key' == column_name.lower():
    #     primary_key = True
    #     ## TODO this is problematic since a response can have more than one output, thus having the same primary key value
    #     ## TODO in order to avoid dup keys in the processor logic, we need to add in a dynamic key hashing function such that
    #     ## it can use the output values as part of the key, not only the input values

    ## TODO we should probably use a DDL builder instead of this old school method

    # calculate the next highest value power of two
    exponent = math.log2(max_length) + 1  # the log of the max size gives us the exponent
    final_max_length = int(math.pow(2, exponent))  # base 2 ^ exponent

    # if the max value is above the max allowed then use the text type instead (or clob)
    if column_type == 'vector':
        if 'dimensions' not in column_header:
            raise Exception(f'column: {column_name} has no dimensions size, column type: {column_type}')

        dimensions = column_header['dimensions']
        column_type = f'VECTOR({dimensions})'
    elif column_type is int:
        column_type = 'INTEGER'
    elif column_type is float:
        column_type = 'NUMBER'
    elif final_max_length >= 4096:
        column_type = 'TEXT'
    else:
        column_type = f"VARCHAR({final_max_length})"

    column_name = clean_string_for_ddl_naming(column_name)

    return f'"{column_name}" {column_type} {"NULL" if column_nullable else "NOT NULL"}'


def calculate_embeddings(text: str):
    return create_embedding(text=text, model_name='st_minilm_l6_v2')


def add_header_columns_and_values_embeddings(input_state: dict,
                                             designated_embedding_columns: dict = None,
                                             data_entry_state: dict = None):

    result_columns = {}

    # fetch a list of keys "column names" from the current data entry (row)
    search_key_mapping = { build_column_name(key): key for key in data_entry_state.keys()}

    def process(target_column_name: str):

        # ensure to clean up the naming convention such that we have accurate matches
        target_column_name = build_column_name(target_column_name)

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
        source_column_value = data_entry_state[source_column_name]

        # skip if the input column does not exist
        if not source_column_value:
            logging.warning(f'unable to create data embedding for null or empty value on'
                            f'source column: {source_column_name}, '
                            f'target column: {target_column_name}, '
                            f'data entry state: {data_entry_state}')
            return

        # otherwise create a new column derived from the target column name + prefixed with _embedding
        target_column_embedding_name = f'{target_column_name}_embedding'

        # create a new embedding function call
        new = {
            #
            # create a new column header with a higher order function to call when the data entry row is iterated over
            target_column_embedding_name: {
                'column_name': target_column_embedding_name,
                'value': routine(func=calculate_embeddings, text=source_column_value),
                'type': 'vector',
                'dimensions': 384
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

def add_header_columns_and_values(input_state: State,
                                  # the initial input state that was passed in, it can be dict of your choice

                                  data_entry_state: State = None,
                                  # passed incase you want to do something with it

                                  input_organized_state: dict = None
                                  # passed incase you need this information during column/value creation
                                  ):
    config = input_state['header']

    return {
        'provider_name': {
            'column_name': 'provider_name',
            'type': str,
            'value': header['provider_name'],
            'max_length': 64
        },
        'model_name': {
            'column_name': 'model_name',
            'type': str,
            'value': header['model_name'],
            'max_length': 64
        }
    }




def create_table_ddl(organized_state: dict):
    headers = organized_state['header']
    columns = headers['columns']
    table_name = headers['table_name']

    # create the column definitions based on the header column information
    # for each column header, invoke the create_column_ddl and returns a
    # list of columns which is then joined
    column_definitions = ',\n\t'.join([create_column_ddl(column_header=column_header)
                                       for column_name, column_header
                                       in columns.items()])

    return f"""
    CREATE EXTENSION IF NOT EXISTS vector;
    
    CREATE TABLE "{table_name}" (
    {column_definitions}
    );
    """.strip()


# Read database URL from environment variable, defaulting to a local PostgreSQL database
DATABASE_URL = os.environ.get("OUTPUT_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")


# Create an asynchronous database engine
async def create_connection():
    return await asyncpg.connect(DATABASE_URL)


async def truncate_table(table_name: str):
    conn = await create_connection()
    try:
        await conn.execute('TRUNCATE TABLE "{table_name}"')
    except Exception as e:
        logging.error(e)
        raise e


async def drop_table(config: StateConfig, table_name: str = None):
    conn = await create_connection()
    try:
        table_name = build_table_name(config) if config else table_name
        if not table_name:
            raise Exception(f'unable to drop table {table_name}, table does not exist')

        await conn.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    except Exception as e:
        logging.error(e)
        raise e

    return True


async def create_table(sql: str):
    conn = await create_connection()
    try:
        await conn.execute(sql)
    except Exception as e:
        logging.warning(e)
        raise e


async def count_table(table_name: str):
    conn = await create_connection()
    try:
        result = await conn.fetch(f'select count(*) as cnt from "{table_name}"')
        data = result.pop()
        return data['cnt']
    except Exception as e:
        logging.error(e)
        return None


async def import_table(organized_data: dict):
    conn = await create_connection()

    header = organized_data['header']
    columns = header['columns']
    table_name = header['table_name']
    row_count = header['row_count']

    def parse_column_name(col_name: str):
        return f'"{col_name}"'

    def parse_column_value(col_val: str):

        if not isinstance(col_val, str):
            return col_val

        if not col_val:
            return ''

        return f"{col_val}"

    # fetch the column names for DML operation, ensuring it gets processed
    _column_names = ','.join([parse_column_name(clean_string_for_ddl_naming(column_name))
                              for column_name, column_header
                              in columns.items()])

    # if idx % batch_size:
    batch_size = 10
    for idx in range(row_count):

        # generate the tuple(.., ..) of values by iterating through the list of columns
        _column_values = tuple(
            parse_column_value(organized_data[column][idx])
            for column, column_header
            in header['columns'].items())

        # TODO parameterize this? or use the Custom Stream IO with the COPY command in PG
        #      below (whenever it gets finished, not needed for now, not a performance bottleneck)
        # await conn.execute("INSERT INTO your_table (column1, column2) VALUES ($1, $2)", value1, value2)

        # create column parameter indexes such that we can parameterize the inserts
        column_params = ','.join([f'${idx}' for idx in range(1, len(columns) + 1)])

        _table_name = f'"{table_name}"'
        sql = f'INSERT INTO {_table_name} ({_column_names}) VALUES ({column_params})'
        try:
            await conn.execute(sql, *_column_values)
        except Exception as e:

            logging.error(e)


async def run_me_state_file(state_input_file: str=None,
                            state=None,
                            additional_values_func=None,
                            ignore_columns: [] = None,
                            drop_tables: bool = False):

    # load the state, organize it for table format and create the DDL for
    # the new organized state such that we can create the backend tables
    if not state:
        state = State.load_state(state_input_file)

    # reorganize the state such that we can use it for persisting it to a database
    # this will create a list of keys columns and an accurate row values (e.g, back-fills empty columns properly)
    organized_state = organize_state_for_database(state=state,
                                                  additional_values_func=additional_values_func,
                                                  ignore_columns=ignore_columns)

    # create the database tables DDL code (but does not execute it)
    create_table_sql = create_table_ddl(organized_state=organized_state)

    # get table name used for dropping, truncating, etc.
    table_name = organized_state['header']['table_name']

    # drop the table if the parameter is set
    # drop_status = False
    if drop_tables:
        status = await drop_table(table_name)
        logging.warning(f'dropped table {table_name}, forced flag: drop_tables, drop status: {status}')

    # for informational purposes
    current_count = await count_table(table_name=table_name)
    append_count = organized_state['header']['row_count']

    # if there is no current count then table does not exist
    if current_count is None:
        logging.info(f'creating table {table_name} for the first time, with definition {create_table_sql}')
        await create_table(sql=create_table_sql)
    else:
        # otherwise just append to the table
        logging.info(f'table {table_name} already exists.'
                     f'cur rows: {current_count} rows, '
                     f'new rows: {append_count} new rows, '
                     f'total rows: {append_count + current_count} rows')

    # import the data into the table
    await import_table(organized_data=organized_state)


async def run_me(drop_tables: bool = False):
    # input_file = '../../dataset/examples/states/715f9bb6ea0cbff9b81a384d879edcc5bc3c04fac8617cf6ae51e32ed976810c.json'
    base_path = '../../states/'

    state_filenames = [
        '5593f05e38e6f276dcf95c0640dbe7082c0804674a7118f5d782059c5875084f.pickle'
    ]

    # load the state, we need this for the additional columns, as it depends
    # on this data for creating provider_name, and model_name column/values
    states = [State]
    for state_filename in state_filenames:
        state = State.load_state(f'{base_path}/{state_filename}')

        if drop_tables:
            await drop_table(config=state.config)

        states.append(state)


    # process each input state separately
    for state in states:

        # wrap around a higher order function that generates additional fields
        # e.g. constants such as provider_name, model_name, and embeddings for question and responses


        # TODO this needs to be a param
        # the columns to process
        designated_embedding_columns = ['input_query',
                                        'input_context',
                                        'question',
                                        'response']

        additional_columns_function_constants = routine(add_header_columns_and_values,
                                                        input_state=state)

        additional_columns_function_embeddings = routine(add_header_columns_and_values_embeddings,
                                                         input_state=state,
                                                         designated_embedding_columns=designated_embedding_columns)

        def combined(*args, **kwargs):
            results_1 = additional_columns_function_constants(**kwargs)
            results_2 = additional_columns_function_embeddings(**kwargs)
            return {**results_1, **results_2}

        additional_columns_function = routine(func=combined)

        # execute the transformation from organized_state to database table
        await run_me_state_file(
            state=state,
            ignore_columns=['input_query_original'],
            additional_values_func=additional_columns_function,
        )

# async handling
asyncio.run(run_me(drop_tables=True))
