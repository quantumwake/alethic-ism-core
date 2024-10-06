import datetime as dt
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel

# from .utils.general_utils import calculate_sha256


class UserProfile(BaseModel):
    user_id: str
    email: Optional[str] = None
    name: Optional[str] = None
    max_agentic_units: Optional[int] = 10000    # HARD STOP - maximum compute units during a billing cycle
    created_date: Optional[dt.datetime] = dt.datetime.utcnow()


class UserProject(BaseModel):
    project_id: Optional[str] = None
    project_name: str
    user_id: str
    created_date: Optional[dt.datetime] = dt.datetime.utcnow()


class WorkflowNode(BaseModel):
    node_id: Optional[str] = None
    node_type: str
    node_label: Optional[str] = None
    project_id: str
    object_id: Optional[str] = None
    position_x: float
    position_y: float
    width: Optional[float] = None
    height: Optional[float] = None


class WorkflowEdge(BaseModel):
    source_node_id: str
    target_node_id: str
    source_handle: str
    target_handle: str
    edge_label: str
    type: str
    animated: bool


class ProcessorStateDirection(Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"


class UsageUnitType(Enum):
    TOKEN = "TOKEN"

class ProcessorStatusCode(Enum):
    # Represents that an entity or task has been created or initialized,
    # but no further action has been taken yet.
    CREATED = "CREATED"

    # Indicates that the entity or task is in the process of being routed
    # or directed to the appropriate destination or handler.
    ROUTE = "ROUTE"

    # Implies that the routing process has been completed, and the entity or
    # task has been successfully directed to the intended destination or handler.
    ROUTED = "ROUTED"

    # Suggests that the entity or task has been placed in a queue,
    # waiting to be processed or executed.
    QUEUED = "QUEUED"

    # Indicates that the entity or task is currently
    # being executed or processed.
    RUNNING = "RUNNING"

    # Implies that the execution or processing of the entity or task
    # is being forcefully terminated
    TERMINATE = "TERMINATE"

    # Suggests that the execution or processing of the entity
    # or task has been intentionally stopped or paused.
    STOPPED = "STOPPED"

    # Indicates that the entity or task has been successfully
    # executed or processed to completion.
    COMPLETED = "COMPLETED"

    # Suggests that an error or failure occurred during the execution
    # or processing of the entity or task, preventing it from being completed successfully.
    FAILED = "FAILED"


class UsageUnit(BaseModel):
    id: Optional[int] = None     # serial id
    transaction_time: Optional[dt.datetime] = None
    project_id: str
    unit_type: UsageUnitType = UsageUnitType.TOKEN
    unit_count: int
    unit_cost: float
    unit_total: float
    reference_id: Optional[str] = None
    reference_label: Optional[str] = None

    # id serial not null primary key,
    # transaction_time timestamp,
    # unit_type usage_unit_type not null,
    # unit_count int not null default 0,
    # unit_cost float null default 0,
    # unit_total  float null default 0,
    # reference_id varchar(36),
    # reference_label varchar(4000)

class InstructionTemplate(BaseModel):
    template_id: Optional[str] = None
    template_path: str
    template_content: str
    template_type: str
    project_id: Optional[str] = None


class ProcessorProvider(BaseModel):
    id: Optional[str] = None
    name: str
    version: str
    class_name: str
    user_id: Optional[str] = None
    project_id: Optional[str] = None


class ProcessorProperty(BaseModel):
    processor_id: str
    name: str
    value: Optional[str] = None


class Processor(BaseModel):
    id: Optional[str] = None
    provider_id: Optional[str] = None
    project_id: str
    status: ProcessorStatusCode = ProcessorStatusCode.CREATED
    properties: Optional[List[ProcessorProperty]] = None


class ProcessorState(BaseModel):

    _internal_id: Optional[int] = None     # internal reference number for tracking of log messages
    id: Optional[str] = None
    processor_id: str
    state_id: str
    direction: ProcessorStateDirection = ProcessorStateDirection.INPUT
    status: ProcessorStatusCode = ProcessorStatusCode.CREATED

    # this does not need to be set, it is mainly used for processing input states
    # the current index is set to the highest index that was completed, only sequence +1,
    # the maximum index is set to the highest index that was completed, irrespective of sequence.
    count: Optional[int] = None
    current_index: Optional[int] = None
    maximum_index: Optional[int] = None

    @property
    def internal_id(self):
        return self._internal_id

    @internal_id.setter
    def internal_id(self, internal_id):
        self._internal_id = internal_id


class MonitorLogEvent(BaseModel):
    log_id: Optional[int] = None
    log_type: str
    log_time: Optional[dt.datetime] = None
    internal_reference_id: Optional[int] = None
    user_id: Optional[str] = None
    project_id: Optional[str] = None
    exception: Optional[str] = None
    data: Optional[str] = None


class UnitType(Enum):
    # You'll need to define the possible values for UsageType here
    # For example:
    TOKEN = "TOKEN"
    COMPUTE = "COMPUTE"
    STORAGE = "STORAGE"


class UnitSubType(Enum):
    # You'll need to define the possible values for UsageType here
    # For example:
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    # Add other types as needed


class Usage(BaseModel):
    id: Optional[int] = None
    transaction_time: Optional[dt.datetime] = dt.datetime.now()

    project_id: Optional[str] = None

    resource_id: str
    resource_type: str

    unit_type: UnitType
    unit_subtype: UnitSubType
    unit_count: int

    metadata: Optional[str] = None

    class Config:
        from_attributes = True


class UsageReport(BaseModel):
    user_id: str
    resource_id: Optional[str] = None
    resource_type: Optional[str] = None
    project_id: Optional[str] = None
    day: Optional[int] = None
    month: Optional[int] = None
    year: Optional[int] = None
    unit_type: Optional[str] = None
    unit_subtype: Optional[str] = None
    total: int
    maximum: int
    total_cost: Optional[float] = 0.0


class UsageReportInstant(BaseModel):
    user_id: Optional[str] = None
    project_id: Optional[str] = None
    total: Optional[float] = 0.0
    total_cost: Optional[float] = 0.0


# Enum for user_session_access_level
class UserSessionAccessLevel(Enum):
    DEFAULT = 'default'
    ADMIN = 'admin'


# BaseModel for the 'session' table
class Session(BaseModel):
    session_id: str
    created_date: dt.datetime
    owner_user_id: str


# BaseModel for the 'session_message' table
class SessionMessage(BaseModel):
    message_id: Optional[int] = None
    session_id: str
    user_id: str        # originated by
    original_content: Optional[str] = None
    executed_content: Optional[str] = None
    message_date: dt.datetime


# BaseModel for the 'user_session_access' table
class UserSessionAccess(BaseModel):
    user_id: str
    session_id: str
    access_level: UserSessionAccessLevel = UserSessionAccessLevel.DEFAULT
    access_date: dt.datetime


class ProcessorStateDetail(ProcessorState, Processor):
    pass
