from typing import Optional, List, Union
from pydantic import BaseModel

from .processor_state import (
    StateDataColumnDefinition,
    State,
    StatusCode,
    InstructionTemplate,
    StateDataKeyDefinition, ProcessorStateDirection
)


class ProcessorProvider(BaseModel):
    id: Optional[str] = None
    name: str
    version: str
    class_name: str
    user_id: Optional[str] = None
    project_id: Optional[str] = None


class Processor(BaseModel):
    id: Optional[str] = None
    provider_id: str
    project_id: str
    processor_status: StatusCode = StatusCode.CREATED


class ProcessorState(BaseModel):
    id: Optional[str] = None
    processor_id: str
    state_id: str
    direction: ProcessorStateDirection = ProcessorStateDirection.INPUT


class ProcessorStateStorage:

    def fetch_templates(self):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_processor(self, processor_id) -> Processor:
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_template(self, template_path: str) -> InstructionTemplate:
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_state_data_by_column_id(self, column_id: int):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_state_columns(self, state_id: str) -> List[StateDataColumnDefinition]:
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_states(self):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_state_by_state_id(self, state_id: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_state_by_name_version(self, name: str, version: str, state_type: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_state(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_state_config(self, state_id: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_processors(self):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_processor_states(self):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def delete_template(self, template_path):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_template(self,
                        template_path: str = None,
                        template_content: str = None,
                        template_type: str = None,
                        instruction_template: InstructionTemplate = None):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_processor_states_by(self, processor_id: str,
                                  input_state_id: str = None,
                                  output_state_id: str = None) -> Union[List[ProcessorState], ProcessorState]:
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def update_processor_state(self, processor_state: ProcessorState):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_processor(self, processor: Processor):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_state_config(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_state_columns(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_state_columns_data(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_state_key_definition(self, state_id: str, definition_type: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_state_primary_key_definition(self, state: State):
        primary_key_definition = state.config.primary_key
        self.insert_state_key_definition(state=state,
                                         key_definition_type='primary_key',
                                         definitions=primary_key_definition)

    def insert_query_state_inheritance_key_definition(self, state: State):
        query_state_inheritance = state.config.query_state_inheritance
        self.insert_state_key_definition(state=state,
                                         key_definition_type='query_state_inheritance',
                                         definitions=query_state_inheritance)

    def insert_state_key_definition(self,
                                    state: State,
                                    key_definition_type: str,
                                    definitions: List[StateDataKeyDefinition]):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def fetch_state_column_data_mappings(self, state_id):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def insert_state_column_data_mapping(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state_basic(self, state_id: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state_columns(self, state_id: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state_data(self, columns: dict):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state_data_mappings(self, state_id: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state(self, state_id: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def save_state(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

