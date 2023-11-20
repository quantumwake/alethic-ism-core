import json
import logging
import pickle
from sre_parse import State

import pydantic

import utils

from enum import Enum as PyEnum
from typing import Any, List, Dict, Optional
from pydantic import BaseModel, field_validator

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
    copy_to_children: bool = pydantic.Field(False)
    input_path: Optional[str] = None
    input_storage: Optional[StateStorage] = None
    output_storage: Optional[StateStorage] = None
    output_path: Optional[str] = None
    output_primary_key_definition: Optional[List[StateDataKeyDefinition]] = None
    include_extra_from_input_definition: Optional[List[StateDataKeyDefinition]] = None

    def __setstate__(self, state):
        super().__setstate__(state)

        # missing values
        self.copy_to_children = self.copy_to_children \
            if 'copy_to_children' in state['__dict__'] else False


    def project_basic_config(self, config: 'StateConfig'):
        self.name = config.name if config.name else self.name

        # TODO rethink this, don't like how this is done as it is confusing.
        #  we have to remap the configuration as it is overwritten
        #  (perhaps we need to disginquish more concretely between the input state and current states)

        if self.copy_to_children:
            self.output_primary_key_definition = config.output_primary_key_definition \
                if config.output_primary_key_definition else self.output_primary_key_definition

            self.include_extra_from_input_definition = config.include_extra_from_input_definition \
                if config.include_extra_from_input_definition else self.include_extra_from_input_definition
        else:
            self.output_primary_key_definition = config.output_primary_key_definition
            self.output_primary_key_definition = config.include_extra_from_input_definition


class StateConfigLM(StateConfig):
    system_template_path: Optional[str] = None
    user_template_path: str
    provider_name: str = None
    model_name: str = None

    def project_basic_config(self, config: 'StateConfigLM'):
        super().project_basic_config(config=config)

        if self.copy_to_children:
            self.system_template_path = config.system_template_path \
                if config.system_template_path else self.system_template_path

            self.user_template_path = config.user_template_path\
                if config.user_template_path else self.user_template_path

            self.provider_name = config.provider_name \
                if config.provider_name else self.provider_name

            self.model_name = config.model_name \
                if config.model_name else self.model_name
        else:
            self.system_template_path = config.system_template_path
            self.user_template_path = config.user_template_path
            self.provider_name = config.provider_name
            self.model_name = config.model_name



class StateDataColumnDefinition(BaseModel):
    name: str                           # Column Name
    data_type: str = 'str'              # Data type found in table
    null: Optional[bool] = True         # Is nullable
    min_length: Optional[int] = None    # Length of min string values
    max_length: Optional[int] = None    # Length of max string values
    dimensions: Optional[int] = None    # Dimensions for vector
    value: Optional[Any] = None

    @field_validator('data_type')
    def convert_type_to_string(cls, v):
        if not isinstance(v, str):
            return 'str'

        return v

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

    def __getitem__(self, item):
        return self.values[item]

    def __setitem__(self, key, value):
        self.values[key] = value

    def add_column_data(self, value: Any):
        if self.values:
            self.values.append(value)
        else:
            self.values = [value]

        # list of values for a given column
        return self.values


class State(BaseModel):
    config: StateConfig
    columns: Dict[str, StateDataColumnDefinition] = None
    data: Dict[str, StateDataRowColumnData] = None
    mapping: Dict[str, StateDataColumnIndex] = None
    count: int = 0

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

            if not new_columns:
                logging.warning(f'*** Unbalanced columns in query state entry, new query state entry '
                                f'contain different columns than the original columns that were initialized. '
                                f'current columns: {self.columns}, '
                                f'new columns: {new_columns}')

            logging.info(f'applying new columns {new_columns}')

        # final check to ensure columns were specified
        if not self.columns:   # if we do not find any columns
            logging.error(f'query state entry does not contain any column information,'
                          f'it must be a dictionary of key/value pairs, where each key is a '
                          f'column name and the value is the data for the record name:value')

    def apply_row_data(self, query_state: dict):
        values = [value for value in query_state.values()]
        row_data = StateDataRowColumnData.model_validate({'values': values})

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

        self.add_row_data(state_key=state_key, row_data=row_data)

    def add_row_data(self, state_key: str, row_data: StateDataRowColumnData):
        row_index = 0
        for column_index, column in enumerate(self.columns.keys()):

            value = row_data[column_index]

            # if the data state is not set, create one
            if not self.data:
                self.data = {
                    column: StateDataRowColumnData(values=[])
                }
            elif column not in self.data:
                self.data[column] = StateDataRowColumnData(values=[])

            column_data = self.data[column]

            column_row_count = len(column_data.values)
            self.count = column_row_count if self.count == 0 else self.count

            # check for unbalanced counts between columns
            if self.count != column_row_count:
                raise Exception(f'rows are unbalanced on column {column} against previously '
                                f'processed columns; expected: {self.count}, got: {column_row_count}')

            # we need the row index for the key <=> index dict
            # always the same irrespective of the column, since
            # we are iterating columns, this number resets
            row_index = column_row_count

            # *note we do this in reverse columns[name] = [all row values]
            # add a new row in the columns data values
            column_data.add_column_data(value)

        # create an index to map back to the exact position in the array
        self.add_row_data_mapping(state_key=state_key, index=row_index)

        # increment the row count
        self.count = self.count + 1

    @staticmethod
    def load_state(input_path: str) -> State:
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

        if utils.has_extension(output_path, ['.pkl', '.pickle']):
            with open(output_path, 'wb') as fio:
                pickle.dump(self, fio)
        elif utils.has_extension(output_path, '.json'):
            with open(output_path, 'w') as fio:
                fio.write(self.model_dump_json())

        else:
            raise Exception(f'Unsupported file type for {output_path}')

if __name__ == '__main__':
    # build a test state
    test_state = State(
        config=StateConfig(
            name='test state 1',
            input_path='../dataset/examples/states/07c5ea7bfa7e9c6ffd93848a9be3c2e712a0e6ca43cc0ad12b6dd24ebd788d6f.json',
            output_path='../dataset/examples/states/',
            # output_path='../dataset/examples/states/184fef148b36325a9f01eff757f0d90af535f4259c105fc612887d5fad34ce11.json',
            output_primary_key_definition=[
                StateDataKeyDefinition(name='query'),
                StateDataKeyDefinition(name='context'),
            ],
            include_extra_from_input_definition=[
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

    test_state.save_state(output_path='../dataset/examples/states/test_state.pickle')
    test_state.save_state(output_path='../dataset/examples/states/test_state.json')

    # when adding a new row you only provide the values, it must match the same
    # number of columns and in the order of the columns that were added, otherwise
    # there will be data / column misalignment
    test_state.add_row_data(StateDataRowColumnData(values=[
        'why are cats so mean?',   # query
        'Education',               # context
        'cats are ....',           # response
        'Instrumentalist',         # analysis_dimension
        45,                        # analysis_dimension_score
    ]))

    test_state.add_row_data(StateDataRowColumnData(values=[
        'why are cats so mean?',  # query
        'Education',  # context
        'cats are cool too ....',  # response
        'Person-Centric',  # analysis_dimension
        88,  # analysis_dimension_score
    ]))

    print(test_state)
