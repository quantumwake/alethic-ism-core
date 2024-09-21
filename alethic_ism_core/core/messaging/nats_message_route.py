import asyncio
import json
import time
from typing import Any, Optional, Callable, Awaitable

import nats
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig
from nats.js.errors import NotFoundError
from pydantic import BaseModel, PrivateAttr
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from .base_message_route_model import BaseRoute, MessageStatus, RouteMessageStatus
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class NATSRoute(BaseRoute, BaseModel):
    # required for NATS subscriber / publisher model
    name: str           # the name of the jetstream
    url: str            # the connection url to the jetstream server
    subject: str        # the channel or subject to listen on
    queue: Optional[str] = None     # the consumer queue / group to join
    jetstream_enabled: Optional[bool] = True

    # internal tracking
    consumer_no: Optional[int] = 1      # a number to identify the subscriber index on the queue

    # internal objects handling the publishing / subscriber model
    _nc: NATS = PrivateAttr(default=None)               # connection
    _js: JetStreamContext = PrivateAttr(default=None)   # jetstream recv/send
    _sub: JetStreamContext.PushSubscription = PrivateAttr(default=None)     # subscriber

    @property # TODO needed? I don't think so
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
        logging.info(f'connecting to route: {self.name}, subject: {self.subject}')
        if self._nc and self._nc.is_connected:
            logging.debug(f'route is already connected, skipping connect on route: {self.name}, subject: {self.subject}')
            return True

        try:
            # connect to the nats core server
            self._nc = NATS()

            logging.debug(f'preparing to connect route: {self.name}, subject: {self.subject}, url {self.url}')
            await self._nc.connect(
                servers=[self.url],
            )

            logging.info(f'connected to route: {self.name}, subject: {self.subject}, url {self.url}')

            # jetstream enablement flag must be set to true for jetstream to work
            if self.jetstream_enabled:
                # Create JetStream context given the nats client connection, if it doesn't exist already
                self._js = self._nc.jetstream()

                try:
                    logging.info(f'initialize:start jetstream for route: {self.name}, subject: {self.subject}')
                    await self._js.find_stream_name_by_subject(self.subject)
                except NotFoundError:
                    logging.info(f'initialize:complete jetstream for route: {self.name}, subject: {self.subject}')
                    await self._js.add_stream(name=self.name, subjects=[self.subject])

            return True
        except Exception as e:
            logging.warning(f"warning, failed to connect and or "
                            f"flush of route: {self.name}, subject: {self.subject}", e)

        return False

    async def request(self, msg: Any) -> Any:
        if not msg:
            return None

        if isinstance(msg, str):
            msg = msg.encode('utf-8')
        elif isinstance(msg, dict):
            msg = json.dumps(msg).encode('utf-8')
        else:
            raise ValueError("Unsupported message type")

        try:
            await self.connect()
            res = await self._nc.request(self.subject, msg, timeout=10.0)
            return res
        except (ErrConnectionClosed, ErrTimeout, ErrNoServers, Exception) as e:
            print("Failed to request-reply message:", e)
        finally:
            pass

        return None

    async def reply(self, msg: Any, reply: str) -> None:
        if not isinstance(msg, Msg):
            raise ValueError(f"invalid msg type received, expected nats.aio.Msg got {type(msg)}")

        await msg.respond(reply)

    async def publish(self, msg: Any) -> RouteMessageStatus:

        if not msg:
            return None

        if isinstance(msg, str):
            msg = msg.encode('utf-8')
        elif isinstance(msg, dict):
            msg = json.dumps(msg).encode('utf-8')
        else:
            raise ValueError("Unsupported message type")

        try:
            await self.connect()

            if self.jetstream_enabled:
                logging.debug(f'preparing to publish data onto route: {self.name}, subject: {self.subject}')
                awk = await self._js.publish(subject=self.subject, payload=msg)
            else:
                await self._nc.publish(subject=self.subject, payload=msg)
                awk = "N/A"

            return RouteMessageStatus(
                id=str(awk),
                status=MessageStatus.QUEUED
            )
        except (ErrConnectionClosed, ErrTimeout, ErrNoServers, Exception) as e:
            print("Failed to send message:", e)
            return RouteMessageStatus(
                message=msg,
                status=MessageStatus.FAILED,
                error=str(e)
            )
        finally:
            pass

    async def subscribe(self, callback: Optional[Callable[[Any], Awaitable[None]]] = None, consumer_no: int = 1):
        await self.connect()

        if callback:
            self._nc.subscribe(subject=self.subject, queue=self.queue, cb=callback)
            return

        logging.info(f'subscriber:start to route: {self.name}, subject: {self.subject}, jetstream_enabled: {self.jetstream_enabled}')
        if self.jetstream_enabled:
            if self.queue:      # cannot have durable consumers on consumer-queues, other consumers will pick up the slack
                durable_name = None
            else:
                durable_name = f"{self.name}_sub_{consumer_no}"

            logging.info(f'subscriber:start to route: {self.name}, subject: {self.subject}')
            self._sub = await self._js.subscribe(
                subject=self.subject,
                queue=self.queue,
                durable=durable_name,
                config=ConsumerConfig(
                    ack_wait=5,
                )
            )
        else:
            self._sub = await self._nc.subscribe(subject=self.subject, queue=self.queue)

        logging.info(f'subscriber:complete for route: {self.name}, subject: {self.subject}, '
                     f'jetstream_enabled: {self.jetstream_enabled}')
    #
    # async def unsubscribe(self):
    #     self._js.cl
    async def consume(self, wait: bool = True) -> [Any, Any]:
        max_iter_exit = 0
        logging.info(f'subscriber:consume for route: {self.name}, subject: {self.subject}, jetstream_enabled: {self.jetstream_enabled}')
        while wait or max_iter_exit <= 3000:     # 3000 at 0.1 seconds = 300 seconds or 5 minutes
            try:
                if self.jetstream_enabled:
                    # JetStream consumption
                    msg = await self._sub.next_msg(timeout=0.1)
                else:
                    # Standard NATS consumption
                    msg = await self._nc.request(self.subject, b'', timeout=0.1)

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
        if not self.jetstream_enabled:
            return  # there is no ack for regular consumer routes

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

    def clone(self, route_config_updates: dict):
        route_json = json.loads(self.model_dump_json())
        route_json = {
            **route_json,
            **route_config_updates
        }
        return NATSRoute(**route_json)

    async def disconnect(self):
        try:
            logging.info(f"starting: disconnect from route: {self.name}, subject: {self.subject}")
            if await self.drain():
                if not self._nc.is_closed:
                    await self._nc.close()

            logging.info(f"completed: disconnect from route: {self.name}, subject: {self.subject}")
        except Exception as e:
            logging.warning("route disconnect error", e)
            pass

    async def flush(self):
        try:
            await self._nc.flush()
        except Exception as e:
            logging.warning(f"unable flush route", e)

    async def drain(self):
        if self._nc and self._nc.is_connected:
            logging.debug(f"starting: route {self.subject} draining")
            await self._nc.drain()
            while self._nc.is_draining:
                time.sleep(1)
            logging.debug(f"completed: route {self.subject} draining")
            return True

        return False

    def __del__(self):
        pass
        # asyncio.get_event_loop().run_until_complete(self.disconnect())
