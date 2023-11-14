import asyncio
import logging
import os
import math
from io import StringIO
from typing import List

import asyncpg

import utils
import dotenv

# load environment variables if any from .env
dotenv.load_dotenv()

# only keep alphanumerical values and spaces, where spaces is converted to an underscore '_'
clean_char_for_ddl_naming = lambda x: (x if x.isalnum() or x == '_' else ' ' if x == '.' or x.isspace() else '')
clean_string_for_ddl_naming = lambda s: "_".join(''.join([clean_char_for_ddl_naming(c) for c in s]).split(' '))


def build_column_name(name: str):
    return clean_string_for_ddl_naming(name).lower()


def build_table_name(header: dict):
    unique_name = header['name'] if 'name' in header else None

    def prefix(name, from_header: dict = header):
        _prefix = from_header[name].strip() if name in from_header else None

        if _prefix:
            return clean_string_for_ddl_naming(_prefix).lower()

        return str()

    if not unique_name:
        provider = prefix('provider_name')
        model_name = prefix('model_name')
        user_template = prefix('name', from_header=header['user_template'])
        system_template = prefix('name', from_header=header['system_template'])

        table_name_appender_list = f"STATE_{provider} {model_name} {user_template} {system_template}".split()
        unique_name = "_".join([x for x in table_name_appender_list if x])

    return clean_string_for_ddl_naming(unique_name).lower()


def organize_state_for_database(state: dict, additional_values_func=None, ignore_columns: [] = None):
    state_data = state['data']
    state_header = state['header']

    # columns = set()
    organized_data = {
        "header": {
            "columns": {
                ## '<column_name>': {
                ## value: [constant value or a routine] (optional)
                ## type: <str,int,float>,
                ## max_length: [min length of input strings for column values] if str (optional)
                ## min_length: [min length of input strings for column values] if str (optional)
                # }
            },
            "row_count": len(state_data),
            "table_name": build_table_name(state_header)
        }
    }

    for idx, data_entry in enumerate(state_data):
        logging.info(f'processing {idx} / {len(state_data)}')

        if not isinstance(data_entry, dict):
            raise Exception(f'Invalid input data entry, it must be a dictionary, '
                            f'received {type(data_entry)} expected {type(dict)}')

        # merge the data entry columns with the table columns to be created
        column_headers = organized_data['header']['columns']

        # this is the list of keys from the current record
        state_data_entry_keys = data_entry.keys()

        # sometimes state['data'] records may differ in their keys, creating a gap in definition
        # we merge them to create a new list of columns from the keys that may not exist yet
        new_column_headers = {
            build_column_name(column): {
                'column_name': build_column_name(column),
                'state_data_entry_key': column,
                'type': str
            }

            # loop through and check lowercase for
            for column in state_data_entry_keys
            if build_column_name(column) not in column_headers and
               build_column_name(column) not in ignore_columns
        }

        # merge the existing columns headers with the specific records columns
        # it would be good practice to try and keep the dataset consistent across
        # available keys/columns, generally we do not want to have key differences
        # between record entries as it leads to many empty/null values
        column_headers = {**column_headers, **new_column_headers}

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

        def get_additional_column_value(column_name: str):
            #
            # check to see if this is a derived column value, either
            # from a constant or a callable function, get the value
            #
            if column in additional_columns:
                additional_column = additional_columns[column]
                additional_column_value = additional_column['value']

                if isinstance(additional_column_value, str):
                    return column_name, additional_column_value
                elif callable(additional_column_value):
                    return column_name, additional_column_value()

                return column_name, additional_column_value

            return column_name, None

        # iterate the global columns, such that we preserve the ordering of data
        # and replace nonexistent column values in the data entry with a blank/null
        # for column, column_header in column_headers.items():

        for col_index, column in enumerate(column_headers.keys()):
            column = column.lower()
            column_header = column_headers[column]

            # if true then it exists in additional columns, otherwise false
            _other_column, value = get_additional_column_value(column)

            if column not in organized_data:
                organized_data[column] = [None for i in range(idx)]

            # try and get the input state key, since this differs from the column name, we use this key to
            # fetch the exact data element we need from the state['data'][row][state_data_entry_key]
            state_data_entry_key = column_header['state_data_entry_key'] if 'state_data_entry_key' in column_header else column

            # override whatever value we have with the one from the dataset
            value = data_entry[state_data_entry_key] if state_data_entry_key in data_entry else value

            # append the value
            value = utils.convert_string_to_instanceof(value)

            # TODO hack or just a check?
            if (type(value) is str and
                    column_header['type'] is int or
                    column_header['type'] is float):
                logging.warning(f'type conversion issue due to invalid input value "{value}" for int or float values on column {column}')
                value = None
            else:
                column_header['type'] = type(value)

            organized_data[column].append(value)

            def calc_min():
                cur = column_header['min_length'] if 'min_length' in column_header else 0
                new = len(value)
                return min(cur, new)

            def calc_max():
                cur = column_header['max_length'] if 'max_length' in column_header else 0
                new = len(value)
                return max(cur, new)

            if not value:
                # any empty value will flag this column as nullable
                column_header['nullable'] = True
            else:
                # only mark it as not nullable if it does not already exist
                column_header['nullable'] = False \
                    if 'nullable' not in column_header \
                    else column_header['nullable']

            # this is later used when creating the column field and rounded up to a power of base 2
            if isinstance(value, str):
                column_header['min_length'] = calc_min()
                column_header['max_length'] = calc_max()

            # update the current column header
            column_headers[column] = column_header

        # update all column headers
        organized_data['header']['columns'] = column_headers

    return organized_data


#
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
    if final_max_length >= 4096:
        column_type = 'TEXT'
    elif column_type is int:
        column_type = 'INTEGER'
    elif column_type is float:
        column_type = 'NUMBER'
    else:
        column_type = f"VARCHAR({final_max_length})"

    column_name = clean_string_for_ddl_naming(column_name)

    return f'"{column_name}" {column_type} {"NULL" if column_nullable else "NOT NULL"}'


# exclude_from_columns: List[str] = None


def add_header_columns_and_values(input_state: dict,
                                  # the initial input state that was passed in, it can be dict of your choice

                                  data_entry_state: dict = None,
                                  # passed incase you want to do something with it

                                  input_organized_state: dict = None
                                  # passed incase you need this information during column/value creation
                                  ):
    header = input_state['header']

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


def routine(func, **fixed_kwargs):
    # The higher-order function
    def wrapped_function(**kwargs):
        # Merge fixed_kwargs (like header_input) with the new kwargs
        all_kwargs = {**fixed_kwargs, **kwargs}
        return func(**all_kwargs)

    return wrapped_function


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


async def drop_table(table_name: str):
    conn = await create_connection()
    try:
        await conn.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    except Exception as e:
        logging.error(e)
        raise e


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


async def run_me_state_file(state_input_file: str, ignore_columns: [] = None, drop_tables: bool = True):
    # load the state, organize it for table format and create the DDL for
    # the new organized state such that we can create the backend tables
    state = utils.load_state(state_input_file)

    column_header_function = routine(add_header_columns_and_values, input_state=state)
    organized_state = organize_state_for_database(state=state,
                                                  additional_values_func=column_header_function,
                                                  ignore_columns=ignore_columns)

    #
    create_table_sql = create_table_ddl(organized_state=organized_state)

    # get table name used for dropping, truncating, etc.
    table_name = organized_state['header']['table_name']

    # drop the table if the parameter is set
    if drop_tables:
        await drop_table(table_name)

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


async def run_me():
    # input_file = '../../dataset/examples/states/715f9bb6ea0cbff9b81a384d879edcc5bc3c04fac8617cf6ae51e32ed976810c.json'
    base_path = '../../dataset/examples/states/'

    state_filenames = [
        '1e1e2abcb60e42da491546c16f1cf7224cb991d899b1ea443d222a86384a88bb.json',
        '9afe7f3daeb42c0139a742b372ece6b1410e3cdfcf95338fc11710beab80f049.json',
        '715f9bb6ea0cbff9b81a384d879edcc5bc3c04fac8617cf6ae51e32ed976810c.json',
        '5415290561bf8559d8671c16017aed476bc463f3ffc55cf6a469a856ef28ac95.json',
        'f50625bfeaade54954c1a42d310f1d5e5e7dd41a49fd279940c3ad74d67e2d5b.json'
    ]

    for state_filename in state_filenames:
        state_file_path = f'{base_path}/{state_filename}'
        await run_me_state_file(
            # drop_tables=True,
            state_input_file=state_file_path,
            ignore_columns=['input_query_original', 'input_context'])


asyncio.run(run_me())


class OrganizedStateCSVStringIO(StringIO):

    def __init__(self, organized_state: dict):

        self.organized_state = organized_state
        self.row_index = 0
        self.row_text_char_index = 0
        self.row_text = None

    @property
    def count(self):
        return int(self.organized_state['row_count'])

    def seek(self, __cookie, __whence=...):
        self.row_index = __cookie

    def read(self, __size=...):

        buffer = []
        for index in range(__size):

            # check whether we need to fetch a new row given the position of the char index within this row
            if not self.row_text or self.row_text_char_index == len(self.row_text):
                self.row_text_char_index = 0
                self.row_text = self.readline()

            # for readability
            row_text = self.row_text

            # add the character to the array
            buffer.append(row_text[self.row_text_char_index])

            # increment the position of the row text column/char index
            self.row_text_char_index = self.row_text_char_index + 1

    def readline(self, __size=...):

        if __size:
            raise NotImplementedError(f'size input in readline not implemented of size {__size}')

        if self.row_index == self.count:
            raise Exception(f'no data found at row {self.row_index} of {self.count}')

        # read the column headers and fetch the relevant data fields at the correct index
        column_headers = self.organized_state['header']['columns']
        row_idx = self.row_index
        line = [self.organized_state[column][row_idx]
                for column, column_header
                in column_headers]

        # join them to be a CSV delimited by | pipe
        self.row_text = "|".join(line)
        self.row_index = self.row_index + 1

        return readline
