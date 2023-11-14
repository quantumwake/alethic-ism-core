import asyncio
import logging
import os
import math

import asyncpg

import utils
import dotenv

# load environment variables if any from .env
dotenv.load_dotenv()

# input_file = '../../dataset/examples/states/715f9bb6ea0cbff9b81a384d879edcc5bc3c04fac8617cf6ae51e32ed976810c.json'
state_input_file = '../../dataset/examples/states/test.json'


def load_state_organized_for_database(state_file: str):
    # state
    state = utils.load_state(state_file)
    data = state['data']

    def create_table_name():
        header = data['header']

        unique_name = header['name'] if 'name' in header else None

        def appender(name):
            return header[name] if name in header else ''

        if not unique_name:
            provider = appender('provider')
            model = appender('model')
            user_template = appender('user_template')
            system_template = appender('system_template')

            unique_name = "_".join("'state_{provider} {model_name} {user_template} {system_template}".split())

        return unique_name



    # columns = set()
    organized_data = {
        "header": {
            "columns": {
                ## '<column_name>': {
                ## type: <str,int,float>,
                ## max_length: [min length of input strings for column values] if str
                ## min_length: [min length of input strings for column values] if str
                # }
            },
            "row_count": len(data),
            "table_name": create_table_name()
        }
    }

    for idx, data_entry in enumerate(data):
        logging.info(f'processing {idx} / {len(data)}')

        if not isinstance(data_entry, dict):
            raise Exception(f'Invalid input data entry, it must be a dictionary, '
                            f'received {type(data_entry)} expected {type(dict)}')

        # merge the data entry columns with the table columns to be created
        column_headers = organized_data['header']['columns']

        data_entry_columns = data_entry.keys()
        data_entry_columns_headers = {
            column: {
                'column_name': column,
                'type': str
            }
            for column in data_entry_columns
        }

        # merge the existing columns headers with the specific records columns
        # it would be good practice to try and keep the dataset consistent across
        # available keys/columns, generally we do not want to have key differences
        # between record entries as it leads to many empty/null values
        column_headers = {**column_headers, **data_entry_columns_headers}
        organized_data['header']['columns'] = column_headers

        # iterate the global columns, such that we preserve the ordering of data
        # and replace nonexistent column values in the data entry with a blank/null
        for column, column_header in column_headers.items():

            # if the column does not exist, create an empty array
            if column not in organized_data:
                organized_data[column] = [None for i in range(idx)]

            value = data_entry[column] if column in data_entry else None
            organized_data[column].append(value)
            value = utils.convert_string_to_instanceof(value)
            column_header['type'] = str(type(value))

            if not value:
                # any empty value will flag this column as nullable
                column_header['nullable'] = True
            else:
                # only mark it as not nullable if it does not already exist
                column_header['nullable'] = False \
                    if 'nullable' not in column_header \
                    else column_header['nullable']

            # check if the value is a string, if so calculate the min and max values for the column
            # this is later used when creating the column field and rounded up to a power of base 2
            if isinstance(value, str):
                column_header['max_length'] = max(len(value),
                                                  column_header['max_length'] if 'max_length' in column_header else 0)
                column_header['min_length'] = max(len(value),
                                                  column_header['min_length'] if 'min_length' in column_header else 0)

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
    min_length = column_header['min_length'] if 'max_length' in column_header else 256
    max_length = column_header['max_length'] if 'max_length' in column_header else 1024

    # if the column name is key then use it as the primary key, otherwise there is no primary key
    # if 'key' == column_name.lower():
    #     primary_key = True
    #     ## TODO this is problematic since a response can have more than one output, thus having the same primary key value
    #     ## TODO in order to avoid dup keys in the processor logic, we need to add in a dynamic key hashing function such that
    #     ## it can use the output values as part of the key, not only the input values

    ## TODO we should probably use a DDL builder instead of this old school method

    # calculate the next highest value
    power_of = math.log2(max_length)
    final_max_length = int(math.pow(2, power_of + 1))

    # if the max value is above the max allowed then use the text type instead (or clob)
    if final_max_length >= 4096:
        column_header['type'] = 'text'

    column_type = f"VARCHAR({final_max_length})"
    if 'int' == column_type:
        column_type = 'INTEGER'
    elif 'float' == column_type:
        column_type = 'NUMBER'

    # only keep alphanumerical values and spaces, where spaces is converted to an underscore '_'
    xxx = lambda x: (x if x.isalnum() or x == '_' else ' ' if x == '.' or x.isspace() else '').upper()
    column_name = ''.join([xxx(x) for x in column_name])

    return f'"{column_name}" {column_type} {"NULL" if column_nullable else "NOT NULL"}'


def create_table_ddl(data: dict):
    headers = data['header']
    columns = headers['columns']
    table_name = headers['table_name']

    # create the column definitions based on the header column information
    # for each column header, invoke the create_column_ddl and returns a
    # list of columns which is then joined
    column_definitions = ',\n\t'.join([create_column_ddl(column_header=column_header)
                                       for column_name, column_header
                                       in columns.items()])

    return f"""
    DROP TABLE IF EXISTS "{table_name}";
    
    CREATE TABLE "{table_name}" (
    {column_definitions}
    );
    """.strip()

organized_state = load_state_organized_for_database(state_file=state_input_file)
create_ddl = create_table_ddl('hello_world_table', organized_state)


# Read database URL from environment variable, defaulting to a local PostgreSQL database
DATABASE_URL = os.environ.get("OUTPUT_DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/postgres")


# Create an asynchronous database engine
async def create_connection():
    return await asyncpg.connect(DATABASE_URL)


async def create_table(create_table_query: str):
    conn = await create_connection()
    await conn.execute(create_table_query)

async def load_data_basic(organized_state: dict)
asyncio.run(create_table(create_ddl))

