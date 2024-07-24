import asyncio
import json
from typing import Any, Optional

from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.errors import NotFoundError
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

    _nc: NATS = PrivateAttr(default=None)
    _js: JetStreamContext = PrivateAttr(default=None)
    _sub: JetStreamContext.PushSubscription = PrivateAttr(default=None)

    @property
    def subject_group(self):
        """
        Returns the subject associated with this route.

        The subject is used to group related routes under a common topic for message consumption.
        This allows multiple route selectors to be processed by the same set of consumers.

        For example, different API calls (e.g., for language, image, and audio processing to openai api)
        might use distinct route selectors but share a common subject. This approach offers
        several benefits:

        1. Flexibility: We can easily swap out route processors without changing the routing logic.
        2. Scalability: It allows for load balancing across multiple consumers subscribed to the same subject.
        3. Maintainability: We can update or replace specific processors without affecting the entire system.

        Example:
        Route selectors like "language/models/openai/gpt3.5", "language/models/openai/gpt4",
        and "image/models/openai/dall-e-3", might share the subject "openai.models".

        This allows us to process all three with the same subscriber (aka, the consumer subscribes to subject via
        the route implementation) or easily redirect one to a new processor if needed.

        Returns:
            str: The subject identifier for this route implementation
        """
        return self.subject  # Assuming the subject is stored in a private attribute

    async def connect(self):
        if self._nc and self._nc.is_connected:
            return True

        try:
            # connect to the nats core server
            self._nc = NATS()
            await self._nc.connect(
                servers=[self.url]
            )

            # Create JetStream context given the nats client connection, if it doesn't exist already
            self._js = self._nc.jetstream()
            try:
                await self._js.find_stream_name_by_subject(self.subject)
            except NotFoundError:
                await self._js.add_stream(name=self.name, subjects=[self.subject])

            return True
        except Exception as e:
            logging.warning(f"connect and flush of route {self.subject} failed", e)

        return False


    async def publish(self, msg: Any) -> RouteMessageStatus:

        if isinstance(msg, str):
            msg = msg.encode('utf-8')
        elif isinstance(msg, dict):
            msg = json.dumps(msg).encode('utf-8')
        else:
            raise ValueError("Unsupported message type")

        try:
            await self.connect()


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
        self.connect()
        self._sub = await self._js.subscribe(subject=self.subject, durable="psub")

    async def consume(self, wait: bool = True) -> [Any, Any]:
        max_iter_exit = 0
        while wait or max_iter_exit <= 3000:     # 3000 at 0.1 seconds = 300 seconds or 5 minutes
            try:
                msg = await self._sub.next_msg(timeout=0.1)
                data = msg.data.decode("utf-8")
                return msg, data
            except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
                raise InterruptedError(e)
            except TimeoutError as e1:
                if not wait:
                    break

                max_iter_exit += 1
            except Exception as e2:
                raise ValueError(e2)

        return None, None

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
