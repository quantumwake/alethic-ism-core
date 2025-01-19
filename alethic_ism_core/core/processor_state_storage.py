import redis
import types
import datetime as dt
from functools import wraps
from typing import Optional, List, Dict, OrderedDict, Any

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
    ProcessorProperty, MonitorLogEvent, ProcessorStatusCode, UsageUnit, UsageReport, Session, SessionMessage,
    UsageReportInstant, StateActionDefinition
)
from .processor_state import (
    State,
    StateDataKeyDefinition,
    StateDataRowColumnData,
    StateDataColumnDefinition,
    StateDataColumnIndex)
from .utils.ismlogging import ism_logger, LOG_LEVEL
from .vault.vault_model import ConfigMap, Vault

logging = ism_logger(__name__)


class FieldConfig:
    def __init__(self, field_name: str, value: Optional[Any], use_in_where: bool, use_in_group_by: bool):
        self.field_name = field_name  # Add field name to the class
        self.value = value
        self.use_in_where = use_in_where
        self.use_in_group_by = use_in_group_by



class UserProfileStorage:

    def fetch_user_profile(self, user_id: str) -> Optional[UserProfile]:
        raise NotImplementedError()

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

    def delete_workflow_edges_by_node_id(self, node_id: str):
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

    def delete_state_column_data_mapping(self, state_id, column_id: int = None) -> int:
        raise NotImplementedError()

    def delete_state_column(self, state_id: str, column_id: int = None) -> int:
        raise NotImplementedError()

    def delete_state_column_data(self, state_id, column_id: int = None) -> int:
        raise NotImplementedError()

    def delete_state_data(self, state_id: str):
        raise NotImplementedError()

    def delete_state_config_key_definition(self, state_id: str, definition_type: str, definition_id: int) -> int:
        raise NotImplementedError()

    def delete_state_config_key_definitions(self, state_id):
        raise NotImplementedError()

    def save_state(self, state: State) -> State:
        raise NotImplementedError()


class UsageStorage:

    # def fetch_usage_instant(self,
    #                         user_id: FieldConfig,
    #                         project_id: Optional[FieldConfig] = None,
    #                         start_date: Optional[FieldConfig] = None,
    #                         end_date: Optional[FieldConfig] = None) -> Optional[UsageReportInstant]:
    #     raise NotImplementedError()

    def fetch_usage_report(
            self,
            user_id: FieldConfig,
            project_id: Optional[FieldConfig] = None,
            resource_id: Optional[FieldConfig] = None,
            resource_type: Optional[FieldConfig] = None,
            year: Optional[FieldConfig] = None,
            month: Optional[FieldConfig] = None,
            day: Optional[FieldConfig] = None,
            unit_type: Optional[FieldConfig] = None,
            unit_subtype: Optional[FieldConfig] = None
    ) -> List[UsageReport]:
        raise NotImplementedError()

    # def fetch_usage_report_user(self, user_id: str) -> List[UsageReport]:
    #     raise NotImplementedError()
    #
    # def fetch_usage_report_project(self, project_id: str) -> List[UsageReport]:
    #     raise NotImplementedError()


class ProcessorStorage:

    def fetch_processors(self, provider_id: str = None, project_id: str = None) \
            -> List[Processor]:
        raise NotImplementedError()

    def fetch_processor(self, processor_id: str) \
            -> Optional[Processor]:
        raise NotImplementedError()

    def delete_processor(self, processor_id: str) -> int:
        raise NotImplementedError()

    def change_processor_status(self, processor_id: str, status: ProcessorStatusCode) -> int:
        raise NotImplementedError()

    def insert_processor(self, processor: Processor) \
            -> Processor:
        raise NotImplementedError()

    def fetch_processor_properties(self, processor_id: str) -> Optional[List[ProcessorProperty]]:
        raise NotImplementedError()

    def fetch_processor_property_by_name(self, processor_id: str, property_name: str):
        raise NotImplementedError()

    def update_processor_property(self, processor_id: str, property_name: str, property_value: str) -> int:
        raise NotImplementedError()

    def insert_processor_properties(self, properties: List[ProcessorProperty]) -> List[ProcessorProperty]:
        raise NotImplementedError()

    def delete_processor_property(self, processor_id: str, name: str) -> int:
        raise NotImplementedError()



class ProcessorStateRouteStorage:

    def fetch_processor_state_routes_by_project_id(self, project_id) \
            -> Optional[List[ProcessorState]]:
        raise NotImplementedError()

    def fetch_processor_state_route_by_route_id(self, route_id: str) \
            -> Optional[ProcessorState]:
        raise NotImplementedError()

    def fetch_processor_state_route(self,
                                    route_id: str = None,
                                    processor_id: str = None,
                                    state_id: str = None,
                                    direction: ProcessorStateDirection = None,
                                    status: ProcessorStatusCode = None) \
            -> Optional[List[ProcessorState]]:
        raise NotImplementedError()

    def insert_processor_state_route(self, processor_state: ProcessorState) \
            -> ProcessorState:
        raise NotImplementedError()

    def delete_processor_state_route(self, route_id: str) -> int:
        raise NotImplementedError()

    def delete_processor_state_routes_by_state_id(self, state_id: str) -> int:
        raise NotImplementedError()


class MonitorLogEventStorage:

    def fetch_monitor_log_events(self,
                                 user_id: str = None, project_id: str = None, reference_id: str = None,
                                 start_date: dt.datetime = None, end_date: dt.datetime = None,
                                 order_by: [str] = None) \
            -> Optional[List[MonitorLogEvent]]:
        raise NotImplementedError()

    def insert_monitor_log_event(self, monitor_log_event: MonitorLogEvent) \
            -> MonitorLogEvent:
        raise NotImplementedError()

    def delete_monitor_log_event(self,
                                 id: str = None,
                                 user_id: str = None,
                                 project_id: str = None,
                                 force: bool = False) -> int:
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


class SessionStorage:

    def create_session(self, user_id: str) -> Session:
        raise NotImplementedError()

    def fetch_session(self, user_id: str, session_id: str) -> Optional[Session]:
        raise NotImplementedError()

    def fetch_user_sessions(self, user_id: str) -> Optional[List[Session]]:
        raise NotImplementedError()

    def user_join_session(self, user_id: str, session_id: str) -> bool:
        raise NotImplementedError()

    def user_unjoin_session(self, user_id: str, session_id:str) -> bool:
        raise NotImplementedError()

    def insert_session_message(self, message: SessionMessage) -> SessionMessage:
        raise NotImplementedError()

    def fetch_session_messages(self, user_id: str, session_id: str) -> Optional[List[SessionMessage]]:
        raise NotImplementedError()

    def delete_session(self, session_id: str) -> int:
        raise NotImplementedError()


class VaultStorage:
    def insert_vault(self, vault: Vault) -> Optional[Vault]:
        raise NotImplementedError()

    def fetch_vaults_by_owner(self, owner_id: str) -> Optional[List[Vault]]:
        raise NotImplementedError()

    def fetch_vault(self, vault_id: str) -> Optional[Vault]:
        raise NotImplementedError()

    def delete_vault(self, vault_id: str) -> int:
        raise NotImplementedError()


class ConfigMapStorage:

    def fetch_config_map(self, config_id: str) -> Optional[ConfigMap]:
        raise NotImplementedError()

    def fetch_config_maps_by_owner(self, owner_id: str) -> Optional[List[ConfigMap]]:
        raise NotImplementedError()

    def delete_config_map(self, config_id: str) -> int:
        raise NotImplementedError()

    def insert_config_map(self, config: ConfigMap) -> Optional[ConfigMap]:
        raise NotImplementedError()


class StateActionStorage:

    def create_state_action(self, action: StateActionDefinition) -> StateActionDefinition:
        raise NotImplementedError()

    def fetch_state_action(self, action_id: str) -> Optional[StateActionDefinition]:
        raise NotImplementedError()

    def fetch_state_actions(self, state_id: str) -> Optional[List[StateActionDefinition]]:
        raise NotImplementedError()

    def delete_state_actions(self, state_id: str) -> int:
        raise NotImplementedError()

    def delete_state_action(self, action_id: str) -> int:
        raise NotImplementedError()


class SessionIdentContext:
    identity: Optional[str] = None


class SessionContext:
    session_id: str

    pass

class RedisSessionStorage(SessionStorage):

    def __init__(self, host='localhost', port=6379, password: str = None, db=0):
        # Initialize the Redis client connection
        self.client = redis.Redis(host=host, port=port, password=password, db=db)

    # def fetch_session_list_by_user(self, session_id: str, user: str):
    #     self.client.hgetall()
    # def insert_session_message_2(self, context: SessionContext):
    #     self.client

    def insert_session_message(self, key, message):
        try:
            # Use RPUSH to append elements to the end of the list in Redis
            logging.info(f"ready to push data onto the cache {key}: {message}")
            print(f"***hello world: start*** {LOG_LEVEL}")
            self.client.rpush(key, message)
            print("***hello world: end***")
            logging.debug(f"successfully r-pushed message to list '{key}': {message}")
        except redis.RedisError as e:
            logging.error(f"error pushing message to Redis: {e}")
            print(f"***hello world: error {e}***")

    def fetch_session_list(self, key):
        try:
            # Retrieve the list stored in Redis under the specified key
            # Use the LRANGE command to get the entire list (start=0, end=-1)
            list_data = self.client.lrange(key, 0, -1)
            logging.debug(f"successfully fetched message list: '{key}")

            # Decode bytes to strings (assuming UTF-8 encoding)
            return [item.decode('utf-8') for item in list_data]
        except redis.RedisError as e:
            logging.debug(f"error pushing message to Redis: {e}")
            return []


class StateMachineStorage(StateStorage,
                          ProcessorStorage,
                          ProcessorStateRouteStorage,
                          ProcessorProviderStorage,
                          WorkflowStorage,
                          TemplateStorage,
                          UserProfileStorage,
                          UserProjectStorage,
                          MonitorLogEventStorage,
                          UsageStorage,
                          SessionStorage,
                          StateActionStorage,
                          VaultStorage,
                          ConfigMapStorage,
                          metaclass=ForwardingStateMachineStorageMeta):
    def __init__(self,
                 state_storage: StateStorage = None,
                 processor_storage: ProcessorStorage = None,
                 processor_state_storage: ProcessorStateRouteStorage = None,
                 processor_provider_storage: ProcessorProviderStorage = None,
                 workflow_storage: WorkflowStorage = None,
                 template_storage: TemplateStorage = None,
                 user_profile_storage: UserProfileStorage = None,
                 user_project_storage: UserProjectStorage = None,
                 monitor_log_event_storage: MonitorLogEventStorage = None,
                 usage_storage: UsageStorage = None,
                 session_storage: SessionStorage = None,
                 state_action_storage: StateActionStorage = None,
                 vault_storage: VaultStorage = None,
                 config_map_storage: ConfigMapStorage = None):

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
        self._delegate_usage_storage = usage_storage
        self._delegate_session_storage = session_storage
        self._delegate_state_action_storage = state_action_storage
        self._delegate_vault_storage = vault_storage
        self._delegate_config_map_storage = config_map_storage
