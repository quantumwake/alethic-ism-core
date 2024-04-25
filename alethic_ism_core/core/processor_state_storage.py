from typing import Optional, List, Dict

from .base_model import (
    UserProject,
    UserProfile,
    WorkflowNode,
    WorkflowEdge,
    InstructionTemplate,
    Processor,
    ProcessorState,
    ProcessorProvider,
    ProcessorStateDirection
)
from .processor_state import (
    State,
    StateDataKeyDefinition,
    StateDataRowColumnData,
    StateDataColumnDefinition,
    StateDataColumnIndex)


class UserProfileStorage:
    def insert_user_profile(self, user_profile: UserProfile):
        raise NotImplemented()


class UserProjectStorage:

    def delete_user_project(self, project_id):
        raise NotImplemented()

    def fetch_user_project(self, project_id: str) \
            -> Optional[UserProject]:
        raise NotImplemented()

    def insert_user_project(self, user_project: UserProject):
        raise NotImplemented()


class TemplateStorage:
    def fetch_templates(self, project_id: str = None) \
            -> Optional[List[InstructionTemplate]]:
        raise NotImplemented()

    def fetch_template(self, template_id: str) \
            -> InstructionTemplate:
        raise NotImplemented()

    def delete_template(self, template_id):
        raise NotImplemented()

    def insert_template(self, template: InstructionTemplate = None) \
            -> InstructionTemplate:
        raise NotImplemented()


class WorkflowStorage:
    def delete_workflow_node(self, node_id):
        raise NotImplemented()

    def fetch_workflow_nodes(self, project_id: str) -> Optional[List[WorkflowNode]]:
        raise NotImplemented()

    def insert_workflow_node(self, node: WorkflowNode):
        raise NotImplemented()

    def delete_workflow_edge(self, source_node_id: str, target_node_id: str):
        raise NotImplemented()

    def fetch_workflow_edges(self, project_id: str) -> Optional[List[WorkflowEdge]]:
        raise NotImplemented()

    def insert_workflow_edge(self, edge: WorkflowEdge):
        raise NotImplemented()


class ProviderStorage:
    def fetch_processor_providers(self,
                                  name: str = None,
                                  version: str = None,
                                  class_name: str = None,
                                  user_id: str = None,
                                  project_id: str = None) \
            -> Optional[List[ProcessorProvider]]:
        raise NotImplemented()

    def insert_processor_provider(self, provider: ProcessorProvider) \
            -> ProcessorProvider:
        raise NotImplemented()

    def delete_processor_provider(self, user_id: str, provider_id: str, project_id: str = None) \
            -> int:
        raise NotImplemented()


class StateStorage:
    def fetch_state_data_by_column_id(self, column_id: int) -> Optional[StateDataRowColumnData]:
        raise NotImplemented()

    def fetch_state_columns(self, state_id: str) \
            -> Optional[Dict[str, StateDataColumnDefinition]]:
        raise NotImplemented()

    def fetch_states(self, project_id: str = None, state_type: str = None)\
            -> Optional[List[State]]:
        raise NotImplemented()

    def fetch_state(self, state_id: str) -> Optional[State]:
        raise NotImplemented()

    def insert_state(self, state: State, config_uuid=False):
        raise NotImplemented()

    def fetch_state_config(self, state_id: str):
        raise NotImplemented()

    def insert_state_config(self, state: State) -> State:
        raise NotImplemented()

    def insert_state_columns(self, state: State):
        raise NotImplemented()

    def insert_state_columns_data(self, state: State, incremental: bool = False):
        raise NotImplemented()

    def fetch_state_key_definition(self, state_id: str, definition_type: str) \
            -> Optional[List[StateDataKeyDefinition]]:
        raise NotImplemented()

    def insert_state_primary_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplemented()

    def insert_query_state_inheritance_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplemented()

    def insert_remap_query_state_columns_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplemented()

    def insert_template_columns_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplemented()

    def insert_state_key_definition(self, state: State, key_definition_type: str,
                                    definitions: List[StateDataKeyDefinition]) \
            -> List[StateDataKeyDefinition]:
        raise NotImplemented()

    def fetch_state_column_data_mappings(self, state_id) \
            -> Dict[str, StateDataColumnIndex]:
        raise NotImplemented()

    def insert_state_column_data_mapping(self, state: State, state_key_mapping_set: set = None):
        raise NotImplemented()

    def load_state_basic(self, state_id: str) \
            -> Optional[State]:
        raise NotImplemented()

    def load_state_columns(self, state_id: str) \
            -> Optional[Dict[str, StateDataColumnDefinition]]:
        raise NotImplemented()

    def load_state_data(self, columns: Dict[str, StateDataColumnDefinition]) \
            -> Optional[Dict[str, StateDataRowColumnData]]:
        raise NotImplemented()

    def load_state_data_mappings(self, state_id: str) \
            -> Dict[str, StateDataColumnIndex]:
        raise NotImplemented()

    def load_state(self, state_id: str, load_data: bool = True):
        raise NotImplemented()

    def delete_state_cascade(self, state_id):
        raise NotImplemented()

    def delete_state(self, state_id):
        raise NotImplemented()

    def delete_state_config(self, state_id):
        raise NotImplemented()

    def delete_state_column_data_mapping(self, state_id):
        raise NotImplemented()

    def delete_state_column(self, state_id):
        raise NotImplemented()

    def delete_state_column_data(self, state_id):
        raise NotImplemented()

    def delete_state_config_key_definitions(self, state_id):
        raise NotImplemented()

    def save_state(self, state: State) -> State:
        raise NotImplemented()


class ProcessorStorage:

    def fetch_processors(self, provider_id: str = None, project_id: str = None) \
            -> List[Processor]:
        raise NotImplemented()

    def fetch_processor(self, processor_id: str) \
            -> Optional[Processor]:
        raise NotImplemented()

    def insert_processor(self, processor: Processor) \
            -> Processor:
        raise NotImplemented()


class ProcessorStateStorage:

    def fetch_processor_state(self, processor_id: str = None, state_id: str = None,
                              direction: ProcessorStateDirection = None) \
            -> Optional[List[ProcessorState]]:
        raise NotImplemented()

    def insert_processor_state(self, processor_state: ProcessorState) \
            -> ProcessorState:
        raise NotImplemented()


class StateMachineStorage(StateStorage,
                          ProcessorStorage,
                          ProcessorStateStorage,
                          ProviderStorage,
                          WorkflowStorage,
                          TemplateStorage,
                          UserProfileStorage,
                          UserProjectStorage):
    pass
