import asyncio
import threading
from typing import Any, Optional

import nats
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from pydantic import PrivateAttr

from ..messaging.base_message_provider import BaseMessagingConsumerProvider
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class NATSMessage:
    def __init__(self, msg):
        self._msg = msg

    def data(self):
        return self._msg.data

    def message_id(self):
        return self._msg.reply

    def partition_key(self):
        return None  # NATS does not have partition_key

    def ordering_key(self):
        return None  # NATS does not have ordering_key

    def topic_name(self):
        return self._msg.subject


class NATSMessagingConsumerProvider(BaseMessagingConsumerProvider):

    url: str
    name: str
    subject: str
    queue: Optional[str] = None

    _nc: NATS = PrivateAttr()
    _js: JetStreamContext = PrivateAttr()
    _sub: JetStreamContext.PushSubscription = PrivateAttr()

    async def connect(self):
        self._nc = nats.NATS()
        self._js = self._nc.jetstream()

        # connect to the nats core server
        await self._nc.connect(self.url)

        # Create JetStream context given the nats client connection
        self._js = self._nc.jetstream()

        # Persist messages on 'foo's subject.
        # await self._js.add_stream(name=self.name, subjects=[self.subject])

        self._sub = await self._js.subscribe(subject=self.subject, durable="psub")


    async def close(self):
        if self._nc:
            await self.main_consumer.unsubscribe()
        await self.nc.drain()

    async def wait(self) -> [Any, Any]:
        try:
            # msg = await self.nc.request(self.message_topic, b'')
            # for msg in msgs:
            #     await msg.ack()
            #     print(msg)
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
            logging.error(f"message id is not set on main consumer {self.message_topic}")

    def get_message_id(self, msg):
        if not isinstance(msg, Msg):
            raise TypeError(f'Invalid message type {type(msg)}')

        return msg.sid

    def friendly_message(self, message: Any):
        if not isinstance(message, NATSMessage):
            raise ValueError(f"unable to process message of type {type(message) if message else None}")

        return str({
            "message_id": message.message_id(),
            "partition_key": message.partition_key(),
            "ordering_key": message.ordering_key(),
            "topic_name": message.topic_name()
        })
