import json
import logging as log
import os
import pickle

from pydantic import BaseModel, field_validator, root_validator, model_validator

from .utils.general_utils import (
    build_template_text_content,
    clean_string_for_ddl_naming,
    calculate_string_list_hash,
    has_extension, calculate_uuid_based_from_string_with_sha256_seed
)

from datetime import datetime as dt
from enum import Enum as PyEnum, Enum
from typing import Any, List, Dict, Optional, Union


class CustomStateUnpickler(pickle.Unpickler):
    def load(self):
        obj = super().load()

        # Modify obj here if it's the desired class

        def update_state_config_properties(_config: Union[StateConfig, StateConfigDB, StateConfigLM]):
            if 'output_primary_key_definition' in _config.__dict__:
                _config.primary_key = _config.output_primary_key_definition
                del _config.output_primary_key_definition

            if 'include_extra_from_input_definition' in _config.__dict__:
                _config.query_state_inheritance = _config.include_extra_from_input_definition
                del _config.include_extra_from_input_definition

            return _config

        if isinstance(obj, StateConfig):
            obj = update_state_config_properties(_config=obj)
        elif isinstance(obj, State):
            obj.config = update_state_config_properties(_config=obj.config)

        return obj

    def find_class(self, module, name):
        try:
            return super().find_class(module, name)
        except ModuleNotFoundError as e:
            if 'State' == name:
                return State
            elif 'StateConfig' == name:
                return StateConfig
            elif 'StateConfigLM' == name:
                return StateConfigLM
            elif 'StateConfigDB' == name:
                return StateConfigDB
            elif 'StateDataColumnDefinition' == name:
                return StateDataColumnDefinition
            elif 'StateDataRowColumnData' == name:
                return StateDataRowColumnData
            elif 'StateDataColumnIndex' == name:
                return StateDataColumnIndex
            elif 'StateDataKeyDefinition' == name:
                return StateDataKeyDefinition
            elif 'ProcessorStatus' == name:
                return StatusCode
            elif 'InstructionTemplate' == name:
                return InstructionTemplate

            raise e


logging = log.getLogger(__name__)



class StateDataKeyDefinition(BaseModel):
    id: Optional[int] = None
    name: str
    alias: Optional[str] = None
    required: Optional[bool] = False
    callable: Optional[bool] = False


class StateStorageClass(PyEnum):
    FILE = "FILE"
    DATABASE = "DATABASE"


def load_state_from_pickle(file_name: str) -> 'State':
    with open(file_name, 'rb') as f:
        unpickler = CustomStateUnpickler(f)
        obj = unpickler.load()
        return obj


#
# class StateStorage(BaseModel):
#     name: str
#     storage_class: StateStorageClass


class StateConfig(BaseModel):
    name: str
    # version: Optional[str] = None  # "Version 0.1"
    storage_class: Optional[str] = "file"
    # output_path: Optional[str] = None  # deprecated for file outputs
    primary_key: Optional[List[StateDataKeyDefinition]] = None
    query_state_inheritance: Optional[List[StateDataKeyDefinition]] = None
    remap_query_state_columns: Optional[List[StateDataKeyDefinition]] = None
    template_columns: Optional[List[StateDataKeyDefinition]] = None


class StateConfigLM(StateConfig):
    user_template_id: str
    system_template_id: Optional[str] = None


class StateConfigCode(StateConfig):
    template_id: Optional[str] = None
    language: str


class StateConfigDB(StateConfig):
    embedding_columns: Optional[List[dict]] = None
    function_columns: Optional[List[dict]] = None
    constant_columns: Optional[List[dict]] = None



class StateDataColumnDefinition(BaseModel):
    id: Optional[int] = None
    name: str  # Column Name
    data_type: str = 'str'  # Data type found in table
    null: Optional[bool] = True  # Is nullable
    min_length: Optional[int] = None  # Length of min string values
    max_length: Optional[int] = None  # Length of max string values
    dimensions: Optional[int] = None  # Dimensions for vector
    value: Optional[Any] = None
    source_column_name: Optional[str] = None  # The source column this column was derived from

    def manual_json(self):
        state = {
            "name": self.name,
            "data_type": self.data_type,
            "null": self.null,
            "min_length": self.min_length,
            "max_length": self.max_length,
            "dimensions": self.dimensions,
            # do not include value
        }
        return state

    class Config:
        json_encoders = {
            type: lambda t: t.__name__  # For serialization to JSON
        }


class StateDataColumnIndex(BaseModel):
    key: str
    values: List[Any]

    def add_index_value(self, value):
        if not self.values:
            self.values = [value]
        else:
            self.values.append(value)

        return self.values


class StateDataRowColumnData(BaseModel):
    values: List[Any]
    count: int = 0

    def __getitem__(self, item):
        return self.values[item]

    def __setitem__(self, key, value):
        self.values[key] = value

    def add_column_data_by_row_index(self, value: Any, row_index: int):
        if not self.values:
            raise IndexError(
                f'no values are specified for this column, column needs to be initialized using the state add_column method')

        self.values[row_index] = value
        return value

    def add_column_data(self, value: Any):
        if self.values:
            self.values.append(value)
        else:
            self.values = [value]

        # check the state to ensure the count is set, otherwise set it
        if 'count' not in self.__dict__ or self.count == 0:
            self.count = len(self.values) if self.values else 0

        # increase count
        self.count = self.count + 1

        # list of values for a given column
        return self.values

    def add_column_data_values(self, values: List[Any]):
        if not values:
            logging.warning(f'add columns data values was called but no data was provided, '
                            f'column information not available at this stage of callstack')
            return

        # check the state to ensure the count is set, otherwise set it
        if 'count' not in self.__dict__ or self.count == 0:
            self.count = len(self.values) if self.values else 0

        self.values.extend(values)
        self.count = self.count + len(values)

        # list of values for a given column
        return self.values


class State(BaseModel):
    id: Optional[str] = None  # primary key, can be generated or directly set
    project_id: Optional[str] = None  # project association id

    config: Optional[Union[StateConfig, StateConfigLM, StateConfigDB]] = None
    columns: Dict[str, StateDataColumnDefinition] = {}
    data: Dict[str, StateDataRowColumnData] = {}
    mapping: Dict[str, StateDataColumnIndex] = {}
    count: int = 0
    persisted_position: Optional[int] = 0
    create_date: Optional[dt] = None
    update_date: Optional[dt] = None
    state_type: Optional[str] = None

    # @model_validator(mode="before")
    # def derive_config_type(cls, state):
    #     return state

    @model_validator(mode="after")
    def derive_state_type(cls, state):
        if state.config:
            state_type = type(state.config).__name__
            state.state_type = state_type

        return state

    @field_validator('config', mode='before')
    def create_config(cls, config_value):
        if isinstance(config_value, StateConfig):
            return config_value

        if config_value and any(key in config_value for key in [
            'provider_name',
            'model_name',
        ]):
            return StateConfigLM(**config_value)
        elif config_value and any(key in config_value for key in [
            'embedding_columns',
            'function_columns',
            'constant_columns'
        ]):
            return StateConfigDB(**config_value)
        else:
            return StateConfig(**config_value)

    def reset(self):
        self.columns = {}
        self.data = {}
        self.mapping = {}
        self.count = 0
        self.create_date = None
        self.update_date = None

    def add_row_data_mapping(self, state_key: str, index: int):
        if not self.mapping:
            self.mapping = {}

        if state_key not in self.mapping:
            # create an array of values associated to this key
            self.mapping[state_key] = StateDataColumnIndex(
                key=state_key,
                values=[index])
        else:
            key_indexes = self.mapping[state_key]
            key_indexes.add_index_value(index)

    def add_column(self, column: StateDataColumnDefinition):
        if not self.columns:
            self.columns = {}

        if not column or not column.name:
            error = f'column definition cannot be null or blank, it must also include a data type and name'
            logging.error(error)
            raise Exception(error)

        if column.name in self.columns:
            logging.warning(f'ignored column {column.name}, column already defined for state {self.config}')
            return

        self.columns[column.name] = column

    def add_columns(self, columns: List[StateDataColumnDefinition]):
        for column in columns:
            self.add_column(column)

    def calculate_columns_definition_hash(self, columns: [str]):
        plain = [key for key in sorted(columns)]
        return calculate_uuid_based_from_string_with_sha256_seed(plain)

    def remap_query_state(self, query_state: dict):

        # if there is no mapping then simply return the same state
        if not self.config.remap_query_state_columns:
            return query_state

        # setup the return mapped query state
        remap = {map.name: map.alias for map in self.config.remap_query_state_columns}
        remapped_query_state = {}

        # iterate current state and attempt to remap if mapping is specified
        for state_item_name, state_item in query_state.items():

            # quick skip field if not found in mapping
            # if the current state item is not in the remap
            if state_item_name not in remap:
                # just add it back in
                remapped_query_state[state_item_name] = state_item
                continue

            # otherwise attempt to remap it
            _map = remap[state_item_name]
            if not _map:
                raise Exception(f'remapping of field {state_item_name} specified without a callable '
                                f'function NEITHER alias. Please specify either a function or an alias using '
                                f'the .alias property in {type(StateDataKeyDefinition)}, current values: {remap}')

            # if it is a function, call it
            alias = _map(query_state=query_state) \
                if callable(_map) else _map

            remapped_query_state[alias] = state_item

        # updated query state to reflect the output state
        return remapped_query_state

    def apply_template_variables(self, query_state: dict):

        if not self.config.template_columns:
            return query_state

        ## map the template variables
        for template_column in self.config.template_columns:
            if template_column.name not in query_state:
                raise Exception(
                    f'template column {template_column} not specified in query state {query_state}, did you remap it using .remap_query_state_columns[]??')

            template_content = query_state[template_column.name]

            # TODO change the alias to something else? maybe more general
            if template_column.alias and callable(template_column.alias):
                template_content = template_column.alias(
                    template_content=template_content,
                    query_state=query_state)

            # map the query state onto the template
            template_content = build_template_text_content(
                template_content=template_content,
                query_state=query_state)

            query_state[template_column.name] = template_content

        return query_state

    def expand_columns(self, column: StateDataColumnDefinition, value_func: Any):

        if column.name in self.columns:
            raise Exception(f'column {column.name} already exists')

        logging.info(f'applying new column {column.name}')
        self.add_column(column)

        # ensure we get a consistent count
        count = implicit_count_with_force_count(self)

        # back-fill using the value_func
        self.data[column.name] = StateDataRowColumnData(
            values=[value_func(row_index)
                    for row_index in range(count)])

    def apply_columns(self, query_state: dict):
        if not query_state:
            raise Exception(f'unable to apply columns on a null or blank query state')

        # calculate the columns definition hash, we use this to compare to ensure there is some consistency
        def new_columns():
            return [
                StateDataColumnDefinition(
                    name=clean_string_for_ddl_naming(name)
                    # data_type=str(type(utils.identify_and_return_value_by_type(value)))   # just guess
                ) for name, value in query_state.items()
                if self.columns is None or name not in self.columns
            ]

        # initial state check
        if not self.columns:
            # add columns if empty or new columns are found in the data entries
            logging.debug(f'identifying applicable columns using query state {query_state.keys()}')
            new_columns = new_columns()
            self.add_columns(new_columns)
            return self.columns

        # consecutive checks to ensure the column key states are consistent
        # calculate the hash of the column names, make sure there are no additional columns
        new_column_definition_hash = calculate_string_list_hash(list(query_state.keys()))
        cur_column_definition_hash = calculate_string_list_hash(list(self.columns.keys()))

        # if the hash is different, then there are likely differences
        if new_column_definition_hash != cur_column_definition_hash:
            new_columns = new_columns()

            if new_columns:
                logging.warning(f'*** Unbalanced columns in query state entry, new query state entry '
                                f'contain different columns than the original columns that were initialized. '
                                f'current columns: {self.columns}, '
                                f'new columns: {new_columns}')

            logging.info(f'applying new columns {new_columns}')
            self.add_columns(new_columns)

            # back-fill
            for new_column in new_columns:
                count = self.count
                self.data[new_column.name] = StateDataRowColumnData(values=[None for i in range(count)])

        # final check to ensure columns were specified
        if not self.columns:  # if we do not find any columns
            logging.error(f'query state entry does not contain any column information,'
                          f'it must be a dictionary of key/value pairs, where each key is a '
                          f'column name and the value is the data for the record name:value')

    def get_query_state_from_row_index(self, index: int):
        # state = state if state else self

        def get_real_value(value):
            # evaluates the value, if it is a callable: string
            return get_column_state_value(
                value=value, **{
                    "state": self,
                    "config": self.config
                }
            )

        query_state = {
            column_name: self.data[column_name][index]
            if not column_header.value
            else get_real_value(column_header.value)
            for column_name, column_header in self.columns.items()
        }
        return query_state

    def get_column_data_from_row_index(self, column_name, index: int):
        return self.data[column_name][index]

    def get_column_data(self, column_name):
        return self.data[column_name]

    def get_row_data_from_query_state(self, query_state: dict):
        # values = [value for value in query_state.values()]
        column_and_value = {
            column: StateDataRowColumnData.model_validate({
                'values': [query_state[column]
                           if column in query_state else None]
            })
            for column in self.columns.keys()
        }

        # TODO revisit this, it is a bit convoluted, kind of the chicken and egg problem
        #  we have a state key defined by the input values since we need to check whether we
        #  we previously processed the input state, as such this is the state key, but
        #  the output key might be different and based on various other factors (maybe not required?)

        if 'state_key' in query_state:
            state_key = query_state['state_key']
        else:
            state_key, state_key_plain = build_state_data_row_key(
                query_state=query_state,
                key_definitions=self.config.primary_key)

        return column_and_value, state_key

    def apply_row_data(self, query_state: dict):
        column_and_value, state_key = self.get_row_data_from_query_state(query_state=query_state)
        return self.add_row_data(state_key=state_key, column_and_value=column_and_value)

    def add_row_data(self, state_key: str, column_and_value: Dict[str, StateDataRowColumnData]):
        row_index = 0
        current_row_count = implicit_count_with_force_count(self) if self.data else 0

        for column_name, column_header in self.columns.items():

            # we will skip any column that has a value, values can be:
            # a. constant string or value of any type
            # b. callable instance, a callable: string starting with callable:<expression>

            if column_header.value:
                logging.debug(f'skipping column: {column_name}, constant and or function value set')
                continue

            if column_name in column_and_value:
                row_column_data = column_and_value[column_name]
            else:
                row_column_data = StateDataRowColumnData(values=[None])

            # if the data state is not set, create one
            if not self.data:
                self.data = {
                    column_name: StateDataRowColumnData(values=[])
                }
            elif column_name not in self.data:  # back-fill rows for new column
                self.data[column_name] = StateDataRowColumnData(values=[None for _ in range(current_row_count)])

            # column_data = self.data[column_name]

            # append the new row to the state.data
            self.data[column_name].add_column_data_values(row_column_data.values)

        # create an index to map back to the exact position in the array
        self.add_row_data_mapping(state_key=state_key, index=self.count)

        # increment the row count
        self.count = self.count + 1

        return True

    @staticmethod
    def load_state(input_path: str) -> 'State':
        if has_extension(input_path, ['.pickle', '.pkl']):
            logging.warning(f'it is advisable to use either a json storage class or a '
                            f'database storage class, the latter of which is preferred.')
            # raise DeprecationWarning(f'pickle file format is deprecated, use the database storage class instead')
            return load_state_from_pickle(file_name=input_path)
        elif has_extension(input_path, ['.json']):
            with open(input_path, 'rb') as fio:
                state = State.model_validate(json.load(fio))
                return state
        else:
            raise Exception(f'unsupported input path type {input_path}')

    def save_state(self, output_path: str = None):
        if not output_path:
            if not self.config.output_path:
                raise FileNotFoundError(f'One of two output_paths must be specified, either in the '
                                        f'state.config.output_path or as part of the output_path argument '
                                        f'into the save_state(..) function')
            elif not os.path.isdir(self.config.output_path):
                logging.debug(f'falling back to using config output path: {self.config.output_path}')
                output_path = self.config.output_path
            else:
                raise Exception(f'Unable to persist to directory output path as specified'
                                f'by the state.config.output_path: {self.state.output_path}')

        self.update_date = dt.utcnow()

        if 'create_date' not in self.__dict__ or not self.create_date:
            self.create_date = dt.utcnow()

        # create the base directory if it does not exist
        dir_path = os.path.dirname(output_path)
        os.makedirs(name=dir_path, exist_ok=True)

        if has_extension(output_path, ['.pkl', '.pickle']):
            # raise DeprecationWarning(f'pickle format is deprecated in favor of a database storage class')
            #
            import pickle
            with open(output_path, 'wb') as fio:
                pickle.dump(self, fio)
        elif has_extension(output_path, '.json'):
            with open(output_path, 'w') as fio:
                fio.write(self.model_dump_json())
        else:
            raise Exception(f'Unsupported file type for {output_path}')


def implicit_count_with_force_count(state: State):
    if not state:
        raise Exception(f'invalid state input, cannot be empty or undefined')

    if not isinstance(state, State):
        raise Exception(f'invalid state type, expected {type(State)}, got {type(state)}')

    count = state.count
    logging.info(f'current state data count: {count} for state config {state.config}')

    if count == 0:
        # force derive the count to see if there are rows
        if not state.columns:
            logging.warning(f'no columns found for state {state.config}, returning zero count results')
            state.count = 0
        else:
            # if the data is blank then return zero count
            if not state.data:
                state.count = 0
            else:
                # otherwise iterate through each column name and look for associated data
                for search_column in state.columns.keys():

                    # if the column is not found in the state data then iterate
                    # until we find a column that has data, such that we can count
                    # otherwise we return zero
                    if search_column not in state.data:
                        pass
                    else:
                        count = len(state.data[search_column].values)
                        state.count = count
                        logging.info(f'force update count of state values: {count}, '
                                     f'no count found but data exists,for  {state.config}')
                        break

    return count

# def add_state_column_value(column: StateDataColumnDefinition,
#                            state_file: str = None,
#                            state: State = None):
#     if not state and not state_file:
#         raise Exception(f'you must specify either a state_file or a load a state using the '
#                         f'State.load_state(..) and pass it as a parameter')
#
#     if state and state_file:
#         raise Exception(f'cannot assign both state_file and state, choose one')
#
#     if state_file:
#         state = State.load_state(state_file)
#
#     if state.columns and column.name in state.columns:
#         raise Exception(f'column {column.name} already exists in state with config: {state.config}')
#
#     state.add_column(column)
#
#     return state


def build_state_data_row_key(query_state: dict, key_definitions: List[StateDataKeyDefinition]):
    try:

        # this will be used as the primary key
        state_key_values = extract_values_from_query_state_by_key_definition(
            key_definitions=key_definitions,
            query_state=query_state)

        # iterate each primary key value pair and create a tuple for hashing
        keys = [(name, value) for name, value in state_key_values.items()]

        # hash the keys as a string in sha256
        return calculate_uuid_based_from_string_with_sha256_seed(str(keys)), keys

    except Exception as e:
        error = (f'error in trying to build state data row key for key definition {key_definitions} '
                 f'and query_state {query_state}. Ensure that the output include keys and query state '
                 f'keys you are trying to use with this key definition are reflective of both the input '
                 f'query state and the response query state. Not that aliases could also cause key match '
                 f'errors, ensure that key combinations are correct. Exception: {e}')

        logging.error(error)
        raise Exception(error)


# def load_state(state_file: str) -> Any:
#     state = {}
#     if not os.path.exists(state_file):
#         raise FileNotFoundError(
#             f'state file {state_file} not found, please use the ensure to use the state template to create the input state file.')
#
#     with open(state_file, 'r') as fio:
#         state = json.load(fio)
#
#         if 'config' not in state:
#             raise Exception(f'Invalid state input, please make sure you define '
#                             f'a basic header for the input state')
#
#         if 'data' not in state:
#             raise Exception(f'Invalid state input, please make sure a data field exists '
#                             f'in the root of the input state')
#
#         data = state['data']
#
#         if not isinstance(data, list):
#             raise Exception('Invalid data input states, the data input must be a list of dictionaries '
#                             'List[dict] where each dictionary is FLAT record of key/value pairs')
#
#     return state


# This function is designed to augment the input dataset with additional data. It offers two methods:
# 1. you can add static column/value pairs to each row.
# 2. you can use a function to generate column/value pairs dynamically.
#
# Example Applications:
#   1. Create an embedding from a column in the input dataset for a specific row.
#   2. Add a fixed value to every row, like a header for input state, such as:
#       - provider_name
#       - model_name
#       - etc.
def get_column_state_value(value: Any, *args, **kwargs):
    if not value:
        return None

    if isinstance(value, str):

        if value.startswith("callable:"):
            func = value[len("callable:"):]
            # TODO security issue - use safe eval
            #   but hell there are a lot of security issues,
            #   this is meant to be running in an isolated
            #   container by tenant, still yet, who knows
            #   what functions are implemented.
            value = eval(func, kwargs)
            return value

        return value

    elif callable(value):
        value = value(*args, **kwargs)
        return value

    return value


def extract_values_from_query_state_by_key_definition(query_state: dict,
                                                      key_definitions: List[StateDataKeyDefinition] = None):
    # if the key config map does not exist then attempt
    # to use the 'query' key as the key value mapping
    if not key_definitions:
        if "query" not in query_state:
            raise Exception(f'query does not exist in query state {query_state}')

        return query_state['query']

    # iterate each parameter name and look it up in the state object
    results = {}
    for key in key_definitions:
        key_name = key.name
        alias = key.alias
        required = key.required if 'required' in key.__dict__ else False

        # if it does not exist, throw an exception to warn the user that the state input is incorrect.
        if key_name not in query_state:
            if required:
                raise Exception(f'Invalid state input for parameter: {key_name}, '
                                f'query_state: {query_state}, '
                                f'key definition: {key}')
            else:
                value = None  # skip the value since it does not exist in the original query state
        else:
            value = query_state[key_name]  # fetch the value from the query state

        if not alias:
            alias = key.name

        # add a new key state value pair,
        # constructing a key defined by values from the query_state
        # given that the key exists and is defined in the config_name mapping
        results[alias] = value

    return results
