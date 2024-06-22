import logging as log
import pickle

from enum import Enum as PyEnum
from datetime import datetime as dt
from typing import Any, List, Dict, Optional, Union
from pydantic import BaseModel, model_validator

from .base_model import ProcessorStatusCode, InstructionTemplate, BaseModelHashable
from .utils.evaluate import safer_evaluate
from .utils.general_utils import (
    build_template_text_content,
    clean_string_for_ddl_naming,
    calculate_string_list_hash,
    calculate_uuid_based_from_string_with_sha256_seed
)


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
                return ProcessorStatusCode
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


class StateConfig(BaseModel):
    name: str
    storage_class: Optional[str] = "database"
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
    required: Optional[bool] = True  # Is nullable
    callable: Optional[bool] = False  # Is nullable
    min_length: Optional[int] = None  # Length of min string values
    max_length: Optional[int] = None  # Length of max string values
    dimensions: Optional[int] = None  # Dimensions for vector
    value: Optional[Any] = None
    source_column_name: Optional[str] = None  # The source column this column was derived from

    def manual_json(self):
        state = {
            "name": self.name,
            "data_type": self.data_type,
            "required": self.required,
            "callable": self.callable,
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

    def build_column_value(self, query_state: dict = None, scope_variable_mappings: dict = None):
        if not self.value:
            return None

        if self.callable:
            allowed_vars = {
                "query_state": query_state,
            }

            if scope_variable_mappings:
                allowed_vars.update(
                    **scope_variable_mappings
                )

            # TODO inject only current state object for evaluation
            # return eval(column_definition.value)
            return safer_evaluate(self.value, allowed_vars=allowed_vars)

        return self.value


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


class State(BaseModelHashable):
    id: Optional[str] = None  # primary key, can be generated or directly set
    project_id: Optional[str] = None  # project association id

    config: Optional[Union[StateConfig, StateConfigLM, StateConfigDB, StateConfigCode]] = None
    columns: Dict[str, StateDataColumnDefinition] = {}
    data: Dict[str, StateDataRowColumnData] = {}
    mapping: Dict[str, StateDataColumnIndex] = {}
    count: int = 0
    persisted_position: Optional[int] = 0
    create_date: Optional[dt] = None
    update_date: Optional[dt] = None
    state_type: Optional[str] = None

    @model_validator(mode="after")
    def derive_state_type(cls, state):
        if state.config:
            state_type = type(state.config).__name__
            state.state_type = state_type

        return state

    @model_validator(mode="before")
    def create_config(cls, value, values):
        if not isinstance(value, dict):
            return value

        # this section is designed to reconstruct only the StateConfig based on State.state_type
        # if the config value does not exist in the root value being processed, then skip it
        if 'config' not in value:
            return value

        # extract the configuration object from the root value (e.g. either instance of StateConfig*, or dict)
        config_value = value['config']

        # if it is a state config type, then return the root object, nothing to do here
        if isinstance(config_value, StateConfig):
            return value

        # Reconstructs the config type based on the state_type
        state_type = value['state_type']
        if state_type == 'StateConfigLM':
            value['config'] = StateConfigLM(**config_value)
        elif state_type == 'StateConfigDB':
            value['config'] = StateConfigDB(**config_value)
        elif state_type == 'StateConfigCode':
            value['config'] = StateConfigCode(**config_value)
        elif state_type == 'StateConfig':
            value['config'] = StateConfig(**config_value)
        else:
            raise NotImplemented(f'unsupported state type {state_type} for {value}')

        return value

    def reset(self):
        self.columns = {}
        self.data = {}
        self.mapping = {}
        self.count = 0
        self.create_date = None
        self.update_date = None

    # def calculate_columns_definition_hash(self, columns: [str]):
    #     plain = [key for key in sorted(columns)]
    #     return calculate_uuid_based_from_string_with_sha256_seed(plain)

    def build_query_state_from_row_data(self, index: int, scope_variable_mappings: dict = None):
        # TODO evaluation of expressions upon expressions is not supported,
        #  this is complex to implement, as such, an expression can only be evaluated
        #  on hard state data and constants, in that order.
        #  (maybe don't do this, not really needed, we can always combine an expression within a single expression)

        # as such, if we have a callable expression definition we can evaluate it in such order.
        # 1. query_state that is stored as rows and columns
        # 2. constant values
        # 3. expressions in the order in which they are added

        # TODO REMOVE - in favor of appending the evaluated and constant columsn directly into the
        #  query_state as a value in the state.data[column].values instead of trying to evaluate it, this helps with the distributed nature of
        #  trying to fix evaluation on the fly issue, in addition, a must be immutable, all elements in the state set should be primed.
        # def get_value(definition: StateDataColumnDefinition):
        #     return self.data[definition.name][index] \
        #         if not definition.value \
        #         else definition.build_column_value()
        #
        # # start with stored values and any constant values
        # query_state = {
        #     name: get_value(definition)
        #     for name, definition in self.columns.items()
        #     if not definition.callable
        # }

        query_state = {
            name: self.data[definition.name][index]
            for name, definition in self.columns.items()
            # if not definition.callable
        }

        # # apply the callable values to the query state for the given row index
        # query_state = {
        #     **query_state,
        #     **{
        #         name: self._build_column_data(
        #             definition=definition,
        #             query_state=query_state,
        #             scope_variable_mappings=scope_variable_mappings
        #         )
        #         for name, definition in self.columns.items()
        #         if definition.callable
        #     }
        # }

        return query_state

    def _build_column_data(self, definition, query_state, scope_variable_mappings=None):

        # if not scope_variable_mappings:
        if scope_variable_mappings is None:
            scope_variable_mappings = {}

        scope_variable_mappings = {
            **scope_variable_mappings,
            "id": self.id,
            "state_type": self.state_type,
            "config": self.config
        }

        return definition.build_column_value(
            query_state=query_state,
            scope_variable_mappings=scope_variable_mappings
        )

    def build_row_key_from_query_state(self, query_state: dict):
        # create the query data row primary key hash
        # the individual state values of the input file
        key_hash, key_plain = build_state_data_row_key(
            # we need the input query to create the primary key
            query_state=query_state,

            # we use the primary key definition because we are creating a primary key
            key_definitions=self.config.primary_key
        )
        return key_hash, key_plain

    def build_row_data_from_query_state(self, query_state: dict):
        column_and_value = {
            column: StateDataRowColumnData.model_validate({
                'values': [query_state[column]
                           if column in query_state else None]
            })
            for column in self.columns.keys()
        }

        if 'state_key' in query_state:
            state_key = query_state['state_key']
        else:
            state_key, state_key_plain = self.build_row_key_from_query_state(query_state=query_state)

        return column_and_value, state_key

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
                raise ValueError(f'remapping of field {state_item_name} specified without a callable '
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
                raise ValueError(
                    f'template column {template_column} not specified in query state {query_state}, '
                    f'did you remap it using .remap_query_state_columns[]??')

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
            raise ValueError(f'column {column.name} already exists')

        logging.info(f'applying new column {column.name}')
        self.add_column(column)

        # ensure we get a consistent count
        count = implicit_count_with_force_count(self)

        # back-fill using the value_func
        self.data[column.name] = StateDataRowColumnData(
            values=[value_func(row_index)
                    for row_index in range(count)])

    def process_and_add_columns(self, query_state: dict):
        """
        Updates the current state object with columns from the given query state. The function checks for missing
        or new columns in the query state and adds them to the existing columns while ensuring that all columns
        remain consistent.

        TODO: the storage class must support column structure updating

        If the state currently has no columns, it initializes them from the query state. If any columns are missing
        or unbalanced (inconsistent) between the current state and the query state, new columns are added, and a
        backfill operation is performed to align existing data.

        Args:
            query_state (dict): A dictionary representing the state data containing column names and their corresponding values.

        Raises:
            ValueError: If the provided `query_state` is None or empty.

        Logs:
            - Debug: Logs the identification of applicable columns from the query state.
            - Warning: Warns if unbalanced columns are detected between the current state and the query state.
            - Info: Logs new columns being applied to the state.
            - Error: If no valid columns are identified in the final state.

        Returns:
            dict: The updated columns after the query state has been applied.

        Example:
            state = State(**params)
            query_state = {
                "name": "Alice",
                "age": 30,
                "location": "New York"
            }
            updated_columns = state.process_and_add_columns(query_state)
        """
        if not query_state:
            raise ValueError(f'unable to apply columns on a null or blank query state')

        # calculate the columns definition hash, we use this to compare to ensure there is some consistency
        # Helper function to create new columns definitions from the query state
        def generate_new_columns():
            return [
                StateDataColumnDefinition(
                    name=clean_string_for_ddl_naming(name)
                )
                for name, value in query_state.items()
                if not self.columns or name not in self.columns
            ]

        # Initial state check for when columns are absent
        if not self.columns:
            logging.debug(f'Identifying applicable columns using query state: {query_state.keys()}')
            new_columns = generate_new_columns()
            self.add_columns(new_columns)
            return self.columns

        # Calculate the hash values to compare current and new columns
        new_column_definition_hash = calculate_string_list_hash(list(query_state.keys()))
        current_column_definition_hash = calculate_string_list_hash(list(self.columns.keys()))

        # if the hash is different, then there are likely differences
        if new_column_definition_hash != current_column_definition_hash:
            new_columns = generate_new_columns()

            if new_columns:
                logging.warning(
                    f'*** Unbalanced columns in query state entry. New query state contains different columns '
                    f'than the original set initialized. Current columns: {self.columns}, '
                    f'New columns: {new_columns}'
                )

            logging.info(f'applying new columns {new_columns}')
            self.add_columns(new_columns)

            # Back-fill new columns with None to ensure consistent data structure
            for new_column in new_columns:
                count = self.count
                self.data[new_column.name] = StateDataRowColumnData(values=[None] * count)

        # Final validation to confirm that columns are now available
        if not self.columns:
            logging.error(
                'Query state entry does not contain any valid column information. '
                'It must be a dictionary of key-value pairs, where each key is a column name, '
                'and the value is the data for that record.'
            )

    def get_column_data_from_row_index(self, column_name, index: int):
        # column_definition = self.columns[column_name]
        # if column_definition.value:
        #     query_state = self.build_query_state_from_row_data(index=index)
        #     return query_state[column_name]
        # else:
        return self.data[column_name][index]

    def get_column_data(self, column_name):
        return self.data[column_name]

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
            raise ValueError(error)

        if column.name in self.columns:
            logging.warning(f'ignored column {column.name}, column already defined for state {self.config}')
            return

        self.columns[column.name] = column

    def add_columns(self, columns: List[StateDataColumnDefinition]):
        for column in columns:
            self.add_column(column)

    def add_row_data(self, state_key: str, column_and_value: Dict[str, StateDataRowColumnData]):
        row_index = 0
        current_row_count = implicit_count_with_force_count(self) if self.data else 0

        for column_name, column_header in self.columns.items():

            # we will skip any column that has a value, values can be:
            # a. constant string or value of any type
            # b. callable instance, a callable: string starting with callable:<expression>

            # if column_header.value:
            #     logging.debug(f'skipping column: {column_name}, constant and or function value set')
            #     continue

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

    def process_and_add_row_data(self, query_state: dict):
        """
        Extracts row data from the query state and adds it to the internal state.

        :param query_state: The state information containing column and value mappings.
        :return: The result of the row data addition.
        """
        column_and_value, state_key = self.build_row_data_from_query_state(query_state=query_state)
        return self.add_row_data(state_key=state_key, column_and_value=column_and_value)

    def has_query_state(self, query_state: dict):
        # make sure that the state is initialized and that there is a data key
        if not self.mapping:
            return False

        if not query_state:
            logging.error(f'received blank or null query state on state id {self.id}')
            raise ReferenceError(f'query state cannot be empty or null')

        # create the input query state entry primary key hash string
        input_query_state_key_hash, input_query_state_key_plain = (
            self.build_row_key_from_query_state(query_state=query_state)
        )

        if input_query_state_key_hash in self.mapping:
            return True

        # otherwise query state entry does not exist in current state set
        logging.debug(f'query {input_query_state_key_hash}, not cached, on config: {self.config}')
        return False

    def apply_query_state(self,
                          query_state: dict,
                          skip_has_query_state: bool = False,
                          scope_variable_mappings: dict = None):
        """
        Applies a query state entry to the state object data rows and updates the indexes.

        :param query_state: The state information to apply.
        :param skip_has_query_state: If True, skips the check for the existence of a query state entry before applying it. Default is False.
        :param scope_variable_mappings: A dictionary key-value pairs used for evaluating callable columns.
                                            For example, eval for column name version: eval(processor_state.version) to
                                            get the model version in a language model configuration.
        :return: The processed query state after the application process.
        """

        # Pre-state apply - perform transformations before applying the state
        query_state = self.pre_state_apply(query_state=query_state)

        # Pre-state apply - applies any constant and or callable value to the query state
        query_state = self.pre_state_apply_callable_and_constant_columns(
            query_state=query_state,
            scope_variable_mappings=scope_variable_mappings
        )

        # Calculate and apply data primary key values to the query state object
        query_state = self.post_state_primary_key_apply(query_state=query_state)

        # Check to ensure query state does not exist already (after the pre state apply step)
        if not skip_has_query_state and self.has_query_state(query_state=query_state):
            return query_state

        # Apply columns as specified in the query state
        self.process_and_add_columns(query_state=query_state)

        # Apply row data from the query state using the helper method
        self.process_and_add_row_data(query_state=query_state)

        # Post-state apply - finalize the function and return the resulting state
        return self.post_state_apply(query_state=query_state)

    #
    # def column_value(self, column_definition: StateDataColumnDefinition):
    #     if column_definition.callable:
    #         # TODO inject only current state object for evaluation
    #         # return eval(column_definition.value)
    #         return safer_evaluate(column_definition.value, allowed_vars={
    #             "query_state": query_state
    #         })
    #
    #     return column_definition.value

    def pre_state_apply_callable_and_constant_columns(self, query_state: dict, scope_variable_mappings: callable = None) -> dict:

        # derive the constant and callable column values and apply them to the query state
        constant_and_callable_query_state = {
            name: self._build_column_data(
                definition=definition,
                query_state=query_state,
                scope_variable_mappings=scope_variable_mappings
            )
            for name, definition in self.columns.items() if definition.value
        }

        # append the constant and callable query state values to the response query state
        query_state = {
            **query_state,
            **constant_and_callable_query_state
        } if constant_and_callable_query_state else query_state

        # return the final query state response with any additional constant and derived (callable) key:value pair
        return query_state

    def post_state_primary_key_apply(self, query_state: dict) -> dict:

        # format the keys, stripping the key name to something more generalized
        output_query_state = {
            clean_string_for_ddl_naming(key): value
            for key, value
            in query_state.items()
        }

        # build the primary key for this query_state
        state_key, state_key_plain = self.build_row_key_from_query_state(
            query_state=output_query_state
        )

        output_query_state = {
            **output_query_state,
            "state_key": state_key,
            "state_key_plain": state_key_plain
        }

        return output_query_state

    def pre_state_apply(self, query_state: dict) -> dict:

        # remapped query state before applying it to the state
        query_state = self.remap_query_state(query_state=query_state)

        # apply any templates using the query state as the primary source of information
        query_state = self.apply_template_variables(query_state=query_state)

        return query_state

    def post_state_apply(self, query_state: dict) -> dict:
        return query_state

    ## TODO move this to a file based state storage provider
    #
    #
    # @staticmethod
    # def load_state(input_path: str) -> 'State':
    #     if has_extension(input_path, ['.pickle', '.pkl']):
    #         logging.warning(f'it is advisable to use either a json storage class or a '
    #                         f'database storage class, the latter of which is preferred.')
    #         # raise DeprecationWarning(f'pickle file format is deprecated, use the database storage class instead')
    #         return load_state_from_pickle(file_name=input_path)
    #     elif has_extension(input_path, ['.json']):
    #         with open(input_path, 'rb') as fio:
    #             state = State.model_validate(json.load(fio))
    #             return state
    #     else:
    #         raise ValueError(f'unsupported input path type {input_path}')
    #
    # def save_state(self, output_path: str = None):
    #     if not output_path:
    #         if not self.config.output_path:
    #             raise FileNotFoundError(f'One of two output_paths must be specified, either in the '
    #                                     f'state.config.output_path or as part of the output_path argument '
    #                                     f'into the save_state(..) function')
    #         elif not os.path.isdir(self.config.output_path):
    #             logging.debug(f'falling back to using config output path: {self.config.output_path}')
    #             output_path = self.config.output_path
    #         else:
    #             raise ValueError(f'Unable to persist to directory output path as specified'
    #                             f'by the state.config.output_path: {self.state.output_path}')
    #
    #     self.update_date = dt.utcnow()
    #
    #     if 'create_date' not in self.__dict__ or not self.create_date:
    #         self.create_date = dt.utcnow()
    #
    #     # create the base directory if it does not exist
    #     dir_path = os.path.dirname(output_path)
    #     os.makedirs(name=dir_path, exist_ok=True)
    #
    #     if has_extension(output_path, ['.pkl', '.pickle']):
    #         # raise DeprecationWarning(f'pickle format is deprecated in favor of a database storage class')
    #         #
    #         import pickle
    #         with open(output_path, 'wb') as fio:
    #             pickle.dump(self, fio)
    #     elif has_extension(output_path, '.json'):
    #         with open(output_path, 'w') as fio:
    #             fio.write(self.model_dump_json())
    #     else:
    #         raise ValueError(f'Unsupported file type for {output_path}')


def implicit_count_with_force_count(state: State):
    if not state:
        raise ValueError(f'invalid state input, cannot be empty or undefined')

    if not isinstance(state, State):
        raise ValueError(f'invalid state type, expected {type(State)}, got {type(state)}')

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
    # this will be used as the primary key
    state_key_values = extract_values_from_query_state_by_key_definition(
        key_definitions=key_definitions,
        query_state=query_state)

    # iterate each primary key value pair and create a tuple for hashing
    keys = [(name, value) for name, value in state_key_values.items()]

    # hash the keys as a string in sha256
    return calculate_uuid_based_from_string_with_sha256_seed(str(keys)), keys


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


# def get_column_state_value(value: Any, *args, **kwargs):
#     if not value:
#         return None
#
#     if isinstance(value, str):
#
#         if value.startswith("callable:"):
#             func = value[len("callable:"):]
#             # TODO security issue - use safe eval
#             #   but hell there are a lot of security issues,
#             #   this is meant to be running in an isolated
#             #   container by tenant, still yet, who knows
#             #   what functions are implemented.
#             value = eval(func, kwargs)
#             return value
#
#         return value
#
#     elif callable(value):
#         value = value(*args, **kwargs)
#         return value
#
#     return value


def extract_values_from_query_state_by_key_definition(
        query_state: dict,
        key_definitions: List[StateDataKeyDefinition] = None
):
    # if the key config map does not exist then attempt
    # to use the 'query' key as the key value mapping
    if not key_definitions:
        return None

    # iterate each parameter name and look it up in the state object
    results = {}
    for key in key_definitions:
        key_name = key.name
        alias = key.alias
        required = key.required if 'required' in key.__dict__ else False

        # if it does not exist, throw an exception to warn the user that the state input is incorrect.
        if key_name not in query_state:
            if required:
                raise ValueError(f'Invalid state input for parameter: {key_name}, '
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
