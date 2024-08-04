import json
from datetime import datetime as dt
from enum import Enum
from typing import Optional, Any
from pydantic import BaseModel


class MessageStatus(Enum):
    QUEUED = "QUEUED"
    FAILED = "FAILED"


class RouteMessageStatus(BaseModel):
    id: Optional[str] = None
    message: Optional[Any] = None
    status: MessageStatus
    error: Optional[str] = None


class BaseRoute(BaseModel):

    selector: str
    creation_date: Optional[dt] = dt.utcnow()

    async def connect(self):
        raise NotImplementedError()

    async def disconnect(self):
        raise NotImplementedError()

    async def subscribe(self):
        raise NotImplementedError()

    async def publish(self, msg: str) -> RouteMessageStatus:
        raise NotImplementedError()

    async def consume(self, wait: bool = True) -> [Any, Any]:
        raise NotImplementedError()

    async def ack(self, message):
        raise NotImplementedError()

    async def flush(self):
        raise NotImplementedError()

    def get_message_id(self, message):
        raise NotImplementedError()

    def friendly_message(self, message: Any):
        raise NotImplementedError()

    def clone(self, route_config_updates: dict):
        raise NotImplementedError()

    @property
    def subject_group(self):
        raise NotImplementedError('the subject property must be defined per route class type, '
                                  'in NATS for example its called a subject, in pulsar or kafka a topic, '
                                  'in kinesis or sqs, something else.')

    @property
    def get_selector_name(self):
        return self.selector
