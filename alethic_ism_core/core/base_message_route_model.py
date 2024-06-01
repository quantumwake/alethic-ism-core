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


class Route(BaseModel):

    topic: str
    manage_topic: Optional[str] = None
    selector: str

    def __init__(self, **data):

        if 'route_config' in data:
            rc = data['route_config']
            data = {
                **data,
                "selector": rc['selector'] if 'selector' in rc else None,
                "topic": rc['topic'] if 'topic' in rc else None,
                "manage_topic": rc['manage_topic'] if 'manage_topic' in rc else None,
            }

        super().__init__(**data)

    def send_message(self, msg: Any) -> RouteMessageStatus:
        raise NotImplementedError()

    @property
    def get_selector_name(self):
        return self.selector

    @property
    def get_topic(self):
        return self.topic

    @property
    def get_manage_topic(self):
        return self.manage_topic
