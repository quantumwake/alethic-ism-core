from abc import abstractmethod
from enum import Enum
from typing import Optional, Any, Type, TypeVar
from pydantic import BaseModel


class MessageStatus(Enum):
    QUEUED = "QUEUED"
    FAILED = "FAILED"


class RouteMessageStatus(BaseModel):
    id: Optional[str] = None
    message: Optional[Any] = None
    status: MessageStatus
    error: Optional[str] = None

#
# T = TypeVar('T', bound='Serializable')
#
# class RouteMessagePayload:
#     @abstractmethod
#     def to_bytes(self) -> bytes:
#         pass
#
#     @classmethod
#     @abstractmethod
#     def from_bytes(cls: Type[T], byte_data: bytes) -> T:
#         pass
#
# class StringMessagePayload(RouteMessagePayload):
#
#     def to_bytes(self) -> bytes:


class Route(BaseModel):

    selector: str

    async def connect(self):
        raise NotImplementedError()

    async def disconnect(self):
        raise NotImplementedError()

    async def send_message(self, msg: str) -> RouteMessageStatus:
        raise NotImplementedError()

    async def flush(self):
        raise NotImplementedError()

    @property
    def get_selector_name(self):
        return self.selector
