import asyncio
import json
from typing import Any, Optional

from nats.js import JetStreamContext
from pydantic import BaseModel, PrivateAttr
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from .base_message_route_model import Route, MessageStatus, RouteMessageStatus
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class NATSRoute(Route, BaseModel):
    name: str
    url: str
    subject: str
    stream: Optional[str] = None
    queue: Optional[str] = None

    _nc: NATS = PrivateAttr()
    _js: JetStreamContext = PrivateAttr()

    async def connect(self):
        try:
            self._nc = NATS()
            await self._nc.connect(
                servers=[self.url]
            )

            # Create JetStream context.
            self._js = self._nc.jetstream()

            # Persist messages on 'foo's subject.
            await self._js.add_stream(name=self.name, subjects=[self.subject])

        except Exception as e:
            logging.warning(f"connect and flush of route {self.subject} failed", e)

    async def send_message(self, msg: Any) -> RouteMessageStatus:

        if isinstance(msg, str):
            msg = msg.encode('utf-8')
        elif isinstance(msg, dict):
            msg = json.dumps(msg).encode('utf-8')
        else:
            raise ValueError("Unsupported message type")

        try:
            awk = await self._js.publish(subject=self.subject, payload=msg, stream=self.stream)
            return RouteMessageStatus(
                id=str(awk),
                status=MessageStatus.QUEUED
            )
        except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
            print("Failed to send message:", e)
            return RouteMessageStatus(
                message=msg,
                status=MessageStatus.FAILED,
                error=str(e)
            )
        finally:
            pass

    async def disconnect(self):
        try:
            if self._nc and self._nc.is_connected:
                self._nc.drain()
        except Exception as e:
            logging.warning("route disconnect error", e)
            pass

    async def flush(self):
        try:
            await self._nc.flush()
        except Exception as e:
            logging.warning(f"unable flush route", e)

    def __del__(self):
        asyncio.run(self.disconnect())
