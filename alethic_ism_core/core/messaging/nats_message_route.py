import json
import random
import time
from typing import Any, Optional, Callable, Awaitable

import nats
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy, StorageType
from nats.js.errors import NotFoundError
from pydantic import BaseModel, PrivateAttr
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from .base_message_route_model import BaseRoute, MessageStatus, RouteMessageStatus
from ..utils.ismlogging import ism_logger

logger = ism_logger(__name__)


class NATSRoute(BaseRoute, BaseModel):
    # required for NATS subscriber / publisher model
    name: str  # the name of the jetstream
    url: str  # the connection url to the jetstream server
    subject: str  # the channel or subject to listen on
    # queue: Optional[str] = None  # the consumer queue / group to join

    jetstream_enabled: Optional[bool] = True

    # internal tracking for consumers, as each consumer needs to be unique
    consumer_id: Optional[str] = "1"  # a number to identify the subscriber index on the queue

    # consumer is actively consuming data flag
    active: Optional[bool] = False

    # internal objects handling the publishing / subscriber model
    _nc: NATS = PrivateAttr(default=None)  # connection
    _js: JetStreamContext = PrivateAttr(default=None)  # jetstream recv/send

    _nc_sub: nats.aio.client.Subscription = PrivateAttr(default=None)  # subscriber for a regular NATS consumer
    _js_pull_sub: JetStreamContext.PullSubscription = PrivateAttr(default=None)  # sub for pull Jetstream consumer

    @property  # TODO needed? I don't think so
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

    async def create_stream(self):
        self._js = self._nc.jetstream()

        logger.info("connecting to jetstream")
        try:
            js_name = await self._js.find_stream_name_by_subject(self.subject)
        except nats.js.errors.NotFoundError:
            # create the stream if it doesn't exist
            logger.info("creating new jetstream")
            stream_config = nats.js.api.StreamConfig(
                name=self.name,
                subjects=[self.subject],
                storage=StorageType.FILE,
            )

            await self._js.add_stream(stream_config)

        logger.info(f"connected to jetstream: {self.name}")
        return self._js

    async def connect(self):
        logger.info(f'connecting to route: {self.name}, subject: {self.subject}')
        if self._nc and self._nc.is_connected:
            logger.debug(f'route is already connected, skipping connect on route: {self.name}, subject: {self.subject}')
            return True

        try:
            # connect to the nats core server
            self._nc = NATS()

            logger.debug(f'connecting to route: {self.name}, subject: {self.subject}, url {self.url}')
            await self._nc.connect(
                servers=[self.url],
            )
            logger.info(f'connected to route: {self.name}, subject: {self.subject}, url {self.url}')

            # jetstream enablement flag must be set to true for jetstream to work
            if self.jetstream_enabled:
                await self.create_stream()

            return True
        except Exception as e:
            logger.warning(f"warning, failed to connect and or "
                           f"flush of route: {self.name}, subject: {self.subject}", e)

        return False

    async def subscribe_request(self):
        async def callback(msg: Msg):
            await self.callback(self, msg, msg.data.decode())

        self._nc_sub = self._nc.subscribe(subject=self.subject, queue=self.queue, cb=callback)

    async def subscribe_pull_jetstream(self):
        if self.consumer_id:
            durable_name = f"{self.name}_sub_{self.consumer_id}"
        else:
            durable_name = self.name

        self._js_pull_sub = await self._js.pull_subscribe(
            subject=self.subject,
            durable=durable_name,
            config=ConsumerConfig(
                ack_wait=5,
                deliver_policy=DeliverPolicy.ALL,
                ack_policy=AckPolicy.EXPLICIT,
                max_ack_pending=1000,
                flow_control=False,
            ),
        )
        return self._js_pull_sub

    async def subscribe_nats(self):
        self._nc_sub = await self._nc.subscribe(
            subject=self.subject,
            queue=self.queue
        )
        return self._nc_sub

    async def subscribe(self) -> bool:
        logger.info(f'subscribe:start to route: {self.name}, subject: {self.subject}, js: {self.jetstream_enabled}')

        if self.jetstream_enabled:
            await self.subscribe_pull_jetstream()
        else:
            await self.subscribe_nats()

        logger.info(f'subscribe:complete to route: {self.name}, subject: {self.subject}, js: {self.jetstream_enabled}')
        return True

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
                logger.debug(f'preparing to publish data onto route: {self.name}, subject: {self.subject}')
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

    async def consume(self, wait: bool = True):
        logger.info(f'consume:start for route: {self.name}, subject: {self.subject}, js: {self.jetstream_enabled}')

        # Backoff parameters
        backoff_base = 0.1  # Starting backoff time in seconds
        backoff_factor = 2  # Exponential backoff factor
        max_backoff = 5     # Maximum backoff time in seconds
        backoff_time = backoff_base
        self.active = True  # the consumer is actively consuming data
        while wait and self.active:  # 50 * 0.1 but exponential backoff can increase this
            try:
                if self.jetstream_enabled:
                    # JetStream consumption
                    msg = await self._js_pull_sub.fetch(batch=10, timeout=backoff_time)   # for pull based
                    if not msg:
                        raise nats.js.errors.FetchTimeoutError("no data received")

                else:
                    # Standard NATS consumption
                    msg = await self._nc.request(self.subject, b'', timeout=backoff_time)
                    if not msg:
                        raise nats.errors.TimeoutError("no data received")

                if isinstance(msg, list):
                    for m in msg:
                        await self.callback(self, m, m.data.decode("utf-8"))
                else:
                    await self.callback(self, msg, msg.data.decode("utf-8"))

                backoff_time = backoff_base  # Reset backoff time
            except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
                raise InterruptedError(e)
            except (nats.js.errors.FetchTimeoutError, nats.aio.errors.ErrTimeout, nats.errors.TimeoutError):
                logger.info(f"no data received, backing off for {backoff_time} seconds...")

                if not wait:
                    break

                # increase backoff time exponentially
                backoff_time = min(backoff_time * backoff_factor, max_backoff)
            except Exception as e2:
                if self.active:
                    raise ValueError(e2)

        # the consumer is not actively consuming data
        self.active = False

    async def ack(self, message):
        # TODO should probably check durability rather then jetstream? kind of confusing but yeah. will figure this out at some point
        if self.jetstream_enabled:
            if message:
                await message.ack()
                return True
            else:
                logger.error(f"message id is not set on main consumer {self.subject}")

        return False

    def get_message_id(self, message: Msg):
        if not isinstance(message, Msg):
            raise TypeError(f'Invalid message type {type(message)}')

        return str(message)

    def friendly_message(self, message: Any):
        return str(message)

    def clone(self, route_config_updates: dict):
        route_json = json.loads(self.model_dump_json())
        route_json = {
            **route_json,
            **route_config_updates
        }
        return NATSRoute(**route_json)

    async def disconnect(self):
        self.active = False

        try:
            logger.info(f"starting: disconnect from route: {self.name}, subject: {self.subject}")
            if await self.drain():
                if not self._nc.is_closed:
                    await self._nc.close()

            logger.info(f"completed: disconnect from route: {self.name}, subject: {self.subject}")
        except Exception as e:
            logger.warning("route disconnect error", e)
            pass

    async def flush(self):
        try:
            await self._nc.flush()
        except Exception as e:
            logger.warning(f"unable flush route", e)

    async def drain(self):
        if self._nc and self._nc.is_connected:
            logger.debug(f"starting: route {self.subject} draining")
            await self._nc.drain()
            while self._nc.is_draining:
                time.sleep(1)
            logger.debug(f"completed: route {self.subject} draining")
            return True

        return False

    def __del__(self):
        pass
        # asyncio.get_event_loop().run_until_complete(self.disconnect())
