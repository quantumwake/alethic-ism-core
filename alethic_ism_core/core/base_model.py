from enum import Enum
from typing import Optional
from pydantic import BaseModel


class UserProfile(BaseModel):
    user_id: str


class UserProject(BaseModel):
    project_id: Optional[str] = None
    project_name: str
    user_id: str


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
    animated: bool



class ProcessorStateDirection(Enum):
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"


class StatusCode(Enum):
    CREATED = "CREATED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    TERMINATED = "TERMINATED"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


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
