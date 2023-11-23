import datetime
import json
import logging as log
import os
import pickle
import utils

from datetime import datetime as dt

from enum import Enum as PyEnum
from typing import Any, List, Dict, Optional, re
from pydantic import BaseModel, field_validator

logging = log.getLogger(__name__)


class StateDataKeyDefinition(BaseModel):
    name: str
    alias: Optional[str] = None


class StateStorageClass(PyEnum):
    FILE = "FILE"
    DATABASE = "DATABASE"


class StateStorage(BaseModel):
    name: str
    storage_class: StateStorageClass


class StateConfig(BaseModel):

    name: str
    input_path: Optional[str] = None
    input_storage: Optional[StateStorage] = None
    output_storage: Optional[StateStorage] = None
    output_path: Optional[str] = None
    output_primary_key_definition: Optional[List[StateDataKeyDefinition]] = None
    include_extra_from_input_definition: Optional[List[StateDataKeyDefinition]] = None
    remap_query_state_columns: Optional[List[StateDataKeyDefinition]] = None
    template_columns: Optional[List[StateDataKeyDefinition]] = None

    #
    # def __setstate__(self, state):
    #     super().__setstate__(state)
    #
    #     # missing values
    #     self.copy_to_children = self.copy_to_children \
    #         if 'copy_to_children' in state['__dict__'] else False


class StateConfigLM(StateConfig):
    system_template_path: Optional[str] = None
    user_template_path: str
    provider_name: str = None
    model_name: str = None


class StateConfigDB(StateConfig):
    embedding_columns: Optional[List[str]] = None


class StateDataColumnDefinition(BaseModel):
    name: str                           # Column Name
    data_type: str = 'str'              # Data type found in table
    null: Optional[bool] = True         # Is nullable
    min_length: Optional[int] = None    # Length of min string values
    max_length: Optional[int] = None    # Length of max string values
    dimensions: Optional[int] = None    # Dimensions for vector
    value: Optional[Any] = None
    source_column_name: Optional[str] = None    # The source column this column was derived from

    @field_validator('data_type')
    def convert_type_to_string(cls, v):
        if not isinstance(v, str):
            return 'str'

        return v

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
    config: StateConfig
    columns: Dict[str, StateDataColumnDefinition] = None
    data: Dict[str, StateDataRowColumnData] = None
    mapping: Dict[str, StateDataColumnIndex] = None
    count: int = 0
    create_date: Optional[dt] = None
    update_date: Optional[dt] = None

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
        if not column or not column.name:
            error = f'column definition cannot be null or blank, it must also include a data type and name'
            logging.error(error)
            raise Exception(error)

        if column.name in self.columns:
            logging.warning(f'ignored column {column.name}, column already defined for state {self.config}')
            return

        self.columns[column.name] = column


    def add_columns(self, columns: List[StateDataColumnDefinition]):
        if not self.columns:
            self.columns = {}

        for column in columns:
            self.add_column(column)


    def calculate_columns_definition_hash(self, columns: [str]):
        plain = [key for key in sorted(columns)]
        return utils.calculate_hash(plain)

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
                raise Exception(f'template column {template_column} not specified in query state {query_state}, did you remap it using .remap_query_state_columns[]??')

            template_content = query_state[template_column.name]

            # TODO change the alias to something else? maybe more general
            if template_column.alias and callable(template_column.alias):
                template_content = template_column.alias(
                    template_content=template_content,
                    query_state=query_state)

            # map the query state onto the template
            template_content = utils.build_template_text_content(
                template_content=template_content,
                query_state=query_state)

            query_state[template_column.name] = template_content

        return query_state


    def apply_columns(self, query_state: dict):
        if not query_state:
            raise Exception(f'unable to apply columns on a null or blank query state')

        # calculate the columns definition hash, we use this to compare to ensure there is some consistency
        def new_columns():
            return [
                StateDataColumnDefinition(
                    name=utils.build_column_name(name)
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
        new_column_definition_hash = utils.calculate_string_list_hash(list(query_state.keys()))
        cur_column_definition_hash = utils.calculate_string_list_hash(list(self.columns.keys()))

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
        if not self.columns:   # if we do not find any columns
            logging.error(f'query state entry does not contain any column information,'
                          f'it must be a dictionary of key/value pairs, where each key is a '
                          f'column name and the value is the data for the record name:value')

    def get_query_state_from_row_index(self, index: str):
        # state = state if state else self

        query_state = {
            column_name: self.data[column_name][index]
            if not column_header.value else column_header.value
            for column_name, column_header in self.columns.items()
        }
        return query_state

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
            state_key, state_key_plain = utils.build_state_data_row_key(
                query_state=query_state,
                key_definitions=self.config.output_primary_key_definition)

        return column_and_value, state_key

    def apply_row_data(self, query_state: dict):
        column_and_value, state_key = self.get_row_data_from_query_state(query_state=query_state)
        return self.add_row_data(state_key=state_key, column_and_value=column_and_value)

    def add_row_data(self, state_key: str, column_and_value: Dict[str, StateDataRowColumnData]):
        row_index = 0
        current_row_count = utils.implicit_count_with_force_count(self) if self.data else 0

        for column_name, column_header in self.columns.items():

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
        if utils.has_extension(input_path, ['.pickle', '.pkl']):
            with open(input_path, 'rb') as fio:
                return pickle.load(fio)
        elif utils.has_extension(input_path, ['.json']):
            with open(input_path, 'rb') as fio:
                state = State.model_validate(json.load(fio))
                return state
        else:
            raise Exception(f'unsupported input path type {input_path}')

    def save_state(self, output_path: str):
        if not output_path:
            raise Exception(f'No output file name specified for state {self}')

        self.update_date = dt.utcnow()

        if 'create_date' not in self.__dict__ or not self.create_date:
            self.create_date = dt.utcnow()

        if utils.has_extension(output_path, ['.pkl', '.pickle']):
            with open(output_path, 'wb') as fio:
                pickle.dump(self, fio)
        elif utils.has_extension(output_path, '.json'):
            with open(output_path, 'w') as fio:
                fio.write(self.model_dump_json())

        else:
            raise Exception(f'Unsupported file type for {output_path}')


def print_state_information(path: str, recursive: bool = False):

    if not os.path.exists(path):
        raise Exception(f'state path does not exist: {path}')

    files = os.listdir(path)

    if not files:
        logging.error(f'no state files found in {path}')
        return

    for nodes in files:
        full_path = f'{path}/{nodes}'
        if os.path.isdir(full_path):
            if recursive:
                logging.info(f'recursive path {full_path}')
                print_state_information(full_path)

            continue


        stat = os.stat(full_path)

        logging.info(f'processing state file with path: {full_path}, '
                     f'created on: {dt.fromtimestamp(stat.st_ctime)}, '
                     f'updated on: {dt.fromtimestamp(stat.st_mtime)}, '
                     f'last access on: {dt.fromtimestamp(stat.st_atime)}')

        state = State.load_state(full_path)
        logging.info("\n\t".join([f'{key}:{value}' for key, value in state.config.model_dump().items()]))


if __name__ == '__main__':
    log.basicConfig(level="DEBUG")
    print_state_information('../states')