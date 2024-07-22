import asyncio
import json
from typing import Any, Optional

from nats.aio.msg import Msg
from nats.js import JetStreamContext
from pydantic import BaseModel, PrivateAttr
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from .base_message_route_model import BaseRoute, MessageStatus, RouteMessageStatus
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class NATSRoute(BaseRoute, BaseModel):
    name: str
    url: str
    subject: str
    stream: Optional[str] = None
    queue: Optional[str] = None

    _nc: NATS = PrivateAttr()
    _js: JetStreamContext = PrivateAttr()
    _sub: JetStreamContext.PushSubscription = PrivateAttr()

    def channel_name(self):
        return self.subject

    async def connect(self):
        try:
            # connect to the nats core server
            self._nc = NATS()
            await self._nc.connect(
                servers=[self.url]
            )

            # Create JetStream context given the nats client connection
            self._js = self._nc.jetstream()

            # Persist messages onto subject.
            await self._js.add_stream(name=self.name, subjects=[self.subject])

        except Exception as e:
            logging.warning(f"connect and flush of route {self.subject} failed", e)

    async def publish(self, msg: Any) -> RouteMessageStatus:

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

    async def subscribe(self):
        self._sub = await self._js.subscribe(subject=self.subject, durable="psub")

    async def consume(self) -> [Any, Any]:
        try:
            msg = await self._sub.next_msg(timeout=2)
            data = msg.data.decode("utf-8")
            return msg, data
        except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
            raise InterruptedError(e)
        except TimeoutError as timeout:
            return None, None
            # threading.Thread.sl
        except Exception as e2:
            raise ValueError(e2)

    async def ack(self, message):
        if message:
            await message.ack()
        else:
            logging.error(f"message id is not set on main consumer {self.subject}")

    def get_message_id(self, message: Msg):
        if not isinstance(message, Msg):
            raise TypeError(f'Invalid message type {type(message)}')

        return str(message)

    def friendly_message(self, message: Any):
        return str(message)

        # return str({
        #     "message_id": message.message_id(),
        #     "partition_key": message.partition_key(),
        #     "ordering_key": message.ordering_key(),
        #     "topic_name": message.topic_name()
        # })

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
