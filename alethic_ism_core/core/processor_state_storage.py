from typing import Optional, List, Union
from pydantic import BaseModel

from .processor_state import (
    StateDataColumnDefinition,
    State,
    ProcessorStatus,
    InstructionTemplate,
    StateDataKeyDefinition
)


class Processor(BaseModel):
    id: str
    type: str


class ProcessorState(BaseModel):
    processor_id: str
    input_state_id: str
    output_state_id: Optional[str] = None
    status: ProcessorStatus = ProcessorStatus.CREATED


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

    def load_state_columns(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state_data(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state_data_mappings(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def load_state(self, state_id: str):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

    def save_state(self, state: State):
        raise NotImplemented(f'requires concrete implementation, see alethic-ism-db class '
                             f'ProcessorStateDatabaseStorage for implementation example.')

