import types
from functools import wraps
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
    ProcessorStateDirection,
    ProcessorProperty, MonitorLogEvent
)
from .processor_state import (
    State,
    StateDataKeyDefinition,
    StateDataRowColumnData,
    StateDataColumnDefinition,
    StateDataColumnIndex)


class UserProfileStorage:
    def insert_user_profile(self, user_profile: UserProfile):
        raise NotImplementedError()


class UserProjectStorage:

    def delete_user_project(self, project_id):
        raise NotImplementedError()

    def fetch_user_project(self, project_id: str) \
            -> Optional[UserProject]:
        raise NotImplementedError()

    def insert_user_project(self, user_project: UserProject):
        raise NotImplementedError()


class TemplateStorage:
    def fetch_templates(self, project_id: str = None) \
            -> Optional[List[InstructionTemplate]]:
        raise NotImplementedError()

    def fetch_template(self, template_id: str) \
            -> InstructionTemplate:
        raise NotImplementedError()

    def delete_template(self, template_id):
        raise NotImplementedError()

    def insert_template(self, template: InstructionTemplate = None) \
            -> InstructionTemplate:
        raise NotImplementedError()


class WorkflowStorage:
    def delete_workflow_node(self, node_id):
        raise NotImplementedError()

    def fetch_workflow_nodes(self, project_id: str) -> Optional[List[WorkflowNode]]:
        raise NotImplementedError()

    def insert_workflow_node(self, node: WorkflowNode) -> WorkflowNode:
        raise NotImplementedError()

    def delete_workflow_edge(self, source_node_id: str, target_node_id: str):
        raise NotImplementedError()

    def fetch_workflow_edges(self, project_id: str) -> Optional[List[WorkflowEdge]]:
        raise NotImplementedError()

    def insert_workflow_edge(self, edge: WorkflowEdge) -> WorkflowEdge:
        raise NotImplementedError()


class ProcessorProviderStorage:
    def fetch_processor_provider(self, id: str) -> Optional[ProcessorProvider]:
        raise NotImplementedError()

    def fetch_processor_providers(self,
                                  name: str = None,
                                  version: str = None,
                                  class_name: str = None,
                                  user_id: str = None,
                                  project_id: str = None) \
            -> Optional[List[ProcessorProvider]]:
        raise NotImplementedError()

    def insert_processor_provider(self, provider: ProcessorProvider) \
            -> ProcessorProvider:
        raise NotImplementedError()

    def delete_processor_provider(self, user_id: str, provider_id: str, project_id: str = None) \
            -> int:
        raise NotImplementedError()


class StateStorage:
    def fetch_state_data_by_column_id(self, column_id: int) -> Optional[StateDataRowColumnData]:
        raise NotImplementedError()

    def fetch_state_columns(self, state_id: str) \
            -> Optional[Dict[str, StateDataColumnDefinition]]:
        raise NotImplementedError()

    def fetch_states(self, project_id: str = None, state_type: str = None)\
            -> Optional[List[State]]:
        raise NotImplementedError()

    def fetch_state(self, state_id: str) -> Optional[State]:
        raise NotImplementedError()

    def insert_state(self, state: State, config_uuid=False):
        raise NotImplementedError()

    def update_state_count(self, state: State) -> State:
        raise NotImplementedError()

    def fetch_state_config(self, state_id: str):
        raise NotImplementedError()

    def insert_state_config(self, state: State) -> State:
        raise NotImplementedError()

    def insert_state_columns(self, state: State):
        raise NotImplementedError()

    def insert_state_columns_data(self, state: State, incremental: bool = False):
        raise NotImplementedError()

    def fetch_state_key_definition(self, state_id: str, definition_type: str) \
            -> Optional[List[StateDataKeyDefinition]]:
        raise NotImplementedError()

    def insert_state_primary_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplementedError()

    def insert_query_state_inheritance_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplementedError()

    def insert_remap_query_state_columns_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplementedError()

    def insert_template_columns_key_definition(self, state: State) \
            -> List[StateDataKeyDefinition]:
        raise NotImplementedError()

    def insert_state_key_definition(self, state: State, key_definition_type: str,
                                    definitions: List[StateDataKeyDefinition]) \
            -> List[StateDataKeyDefinition]:
        raise NotImplementedError()

    def fetch_state_column_data_mappings(self, state_id) \
            -> Dict[str, StateDataColumnIndex]:
        raise NotImplementedError()

    def insert_state_column_data_mapping(self, state: State, state_key_mapping_set: set = None):
        raise NotImplementedError()

    def load_state_basic(self, state_id: str) \
            -> Optional[State]:
        raise NotImplementedError()

    def load_state_columns(self, state_id: str) \
            -> Optional[Dict[str, StateDataColumnDefinition]]:
        raise NotImplementedError()

    def load_state_data(self, columns: Dict[str, StateDataColumnDefinition]) \
            -> Optional[Dict[str, StateDataRowColumnData]]:
        raise NotImplementedError()

    def load_state_data_mappings(self, state_id: str) \
            -> Dict[str, StateDataColumnIndex]:
        raise NotImplementedError()

    def load_state(self, state_id: str, load_data: bool = True) -> Optional[State]:
        raise NotImplementedError()

    def delete_state_cascade(self, state_id):
        raise NotImplementedError()

    def delete_state(self, state_id):
        raise NotImplementedError()

    def delete_state_config(self, state_id):
        raise NotImplementedError()

    def reset_state_column_data_zero(self, state_id, zero: int = 0) -> int:
        raise NotImplementedError()

    def delete_state_column_data_mapping(self, state_id):
        raise NotImplementedError()

    def delete_state_column(self, state_id):
        raise NotImplementedError()

    def delete_state_column_data(self, state_id):
        raise NotImplementedError()

    def delete_state_data(self, state_id):
        raise NotImplementedError()

    def delete_state_config_key_definition(self, state_id: str, definition_type: str, definition_id: int) -> int:
        raise NotImplementedError()

    def delete_state_config_key_definitions(self, state_id):
        raise NotImplementedError()

    def save_state(self, state: State) -> State:
        raise NotImplementedError()


class ProcessorStorage:

    def fetch_processors(self, provider_id: str = None, project_id: str = None) \
            -> List[Processor]:
        raise NotImplementedError()

    def fetch_processor(self, processor_id: str) \
            -> Optional[Processor]:
        raise NotImplementedError()

    def insert_processor(self, processor: Processor) \
            -> Processor:
        raise NotImplementedError()

    def fetch_processor_properties(self, processor_id: str) -> Optional[List[ProcessorProperty]]:
        raise NotImplementedError()

    def insert_processor_properties(self, properties: List[ProcessorProperty]) -> List[ProcessorProperty]:
        raise NotImplementedError()

    def delete_processor_property(self, processor_id: str, name: str) -> int:
        raise NotImplementedError()


class ProcessorStateStorage:

    def fetch_processor_states_by_project_id(self, project_id) \
            -> Optional[List[ProcessorState]]:
        raise NotImplementedError()

    def fetch_processor_state(self, processor_id: str = None, state_id: str = None,
                              direction: ProcessorStateDirection = None) \
            -> Optional[List[ProcessorState]]:
        raise NotImplementedError()

    def insert_processor_state(self, processor_state: ProcessorState) \
            -> ProcessorState:
        raise NotImplementedError()


class MonitorLogEventStorage:

    def fetch_monitor_log_events(self, internal_reference_id: int = None, user_id: str = None, project_id: str = None) \
            -> Optional[List[MonitorLogEvent]]:
        raise NotImplementedError()

    def insert_monitor_log_event(self, monitor_log_event: MonitorLogEvent) \
            -> MonitorLogEvent:
        raise NotImplementedError()


# Metaclass to Add Forwarding Methods Dynamically
class ForwardingStateMachineStorageMeta(type):
    def __new__(cls, name, bases, dct):
        """Create a new class object."""
        return super().__new__(cls, name, bases, dct)

    def __call__(cls, *args, **kwargs):
        """
        Called to create an instance of the class.

        This method will dynamically bind forwarding methods from delegate objects
        to the new instance based on the attributes prefixed with '_delegate_'.
        """
        # Create the instance via the original constructor
        instance = super().__call__(*args, **kwargs)

        # Identify delegate attributes prefixed with "_delegate_"
        delegates = {k: v for k, v in vars(instance).items() if k.startswith('_delegate_')}

        for delegate_attr, delegate_instance in delegates.items():
            # Loop through all attributes of the delegate instance
            for method_name in dir(delegate_instance):
                # Skip private methods and existing attributes
                if method_name.startswith('_'):
                    continue

                # Retrieve the delegate method (already bound to the delegate instance)
                method = getattr(delegate_instance, method_name)

                # Ensure that the method is callable
                if callable(method):
                    # Create a forwarder function to wrap the delegate method
                    def make_forwarder(bound_method):
                        @wraps(bound_method)
                        def forwarder(self, *args, **kwargs):
                            return bound_method(*args, **kwargs)

                        return forwarder

                    # Generate the forwarding method and bind it to the new instance
                    forwarder_method = make_forwarder(method)

                    # Override or create the method on the new instance, ensuring the first parameter is `self`
                    # We use types.MethodType to correctly bind the forwarding method to the instance
                    setattr(instance, method_name, types.MethodType(forwarder_method, instance))

        return instance


class StateMachineStorage(StateStorage,
                          ProcessorStorage,
                          ProcessorStateStorage,
                          ProcessorProviderStorage,
                          WorkflowStorage,
                          TemplateStorage,
                          UserProfileStorage,
                          UserProjectStorage,
                          MonitorLogEventStorage,
                          metaclass=ForwardingStateMachineStorageMeta):
    def __init__(self,
                 state_storage: StateStorage = None,
                 processor_storage: ProcessorStorage = None,
                 processor_state_storage: ProcessorStateStorage = None,
                 processor_provider_storage: ProcessorProviderStorage = None,
                 workflow_storage: WorkflowStorage = None,
                 template_storage: TemplateStorage = None,
                 user_profile_storage: UserProfileStorage = None,
                 user_project_storage: UserProjectStorage = None,
                 monitor_log_event_storage: MonitorLogEventStorage = None):

        # Assign the delegates dynamically via constructor parameters
        self._delegate_state_storage = state_storage
        self._delegate_processor_storage = processor_storage
        self._delegate_processor_state_storage = processor_state_storage
        self._delegate_processor_provider_storage = processor_provider_storage
        self._delegate_workflow_storage = workflow_storage
        self._delegate_template_storage = template_storage
        self._delegate_user_profile_storage = user_profile_storage
        self._delegate_user_project_storage = user_project_storage
        self._delegate_monitor_log_event_storage = monitor_log_event_storage
