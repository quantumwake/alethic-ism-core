import json
import time
from typing import Any, Optional

import nats
from nats.aio.msg import Msg
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy, StorageType, RetentionPolicy
from nats.js.errors import NotFoundError
from pydantic import BaseModel, PrivateAttr
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

from ismcore.messaging.base_message_route_model import BaseRoute, RouteMessageStatus, MessageStatus
from ismcore.utils.ism_logger import ism_logger

logger = ism_logger(__name__)


class NATSRoute(BaseRoute, BaseModel):
    # required for NATS subscriber / publisher model
    name: str  # the name of the jetstream
    url: str  # the connection url to the jetstream server
    subject: str  # the channel or subject to listen on
    queue: Optional[str] = None  # the consumer queue / group to join

    # timeouts and batching
    batch_size: Optional[int] = 1  # number of messages to batch before processing
    ack_wait: Optional[int] = 30  # time to wait for ack before considering the message failed

    jetstream_enabled: Optional[bool] = True

    # concurrency settings
    concurrent_enabled: Optional[bool] = False  # enable concurrent message handling
    concurrent_max_workers: Optional[int] = 10  # max concurrent handlers (semaphore limit)
    concurrent_priority_enabled: Optional[bool] = False  # enable separate high priority queue
    concurrent_max_workers_high: Optional[int] = 5  # dedicated pool for high priority messages

    # internal tracking for consumers, as each consumer needs to be unique
    consumer_id: Optional[str] = "1"  # a number to identify the subscriber index on the queue

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

    def get_publish_subject(self, priority: str = None, partition_key: str = None) -> str:
        """
        Get the subject to publish messages to.

        For base NATSRoute, returns the configured subject directly.
        Subclasses (e.g., NATSRouteConcurrent) override this to support
        priority-based routing.

        Args:
            priority: Ignored in base class. Used by concurrent routes for "high"/"low" routing.
            partition_key: Optional key for partitioning (e.g., project_id).

        Returns:
            The subject string to publish to.
        """
        return self.subject

    async def create_stream(self):
        """
        Create or update JetStream stream to accept both exact subject
        and partitioned/priority subjects ({subject}.>).
        """
        self._js = self._nc.jetstream()
        required_subjects = [self.subject, f"{self.subject}.>"]

        logger.info(f"ensuring jetstream stream: {self.name}")
        try:
            # Check if stream exists
            stream_info = await self._js.stream_info(self.name)
            current_subjects = stream_info.config.subjects or []

            # Update if subjects are missing
            missing = [s for s in required_subjects if s not in current_subjects]
            if missing:
                logger.info(f"updating stream {self.name} to add subjects: {missing}")
                updated_subjects = list(set(current_subjects + required_subjects))
                await self._js.update_stream(
                    config=nats.js.api.StreamConfig(
                        name=self.name,
                        subjects=updated_subjects,
                        storage=stream_info.config.storage,
                        retention=stream_info.config.retention,
                    )
                )
        except nats.js.errors.NotFoundError:
            # Create new stream
            logger.info(f"creating new jetstream: {self.name}")
            stream_config = nats.js.api.StreamConfig(
                name=self.name,
                subjects=required_subjects,
                storage=StorageType.FILE,
                retention=RetentionPolicy.WORK_QUEUE
            )
            await self._js.add_stream(stream_config)

        logger.info(f"jetstream ready: {self.name}")
        return self._js

    async def connect(self):
        """
        Establish connection to NATS server.

        Creates a new connection and initializes JetStream if enabled.
        For most use cases, prefer _ensure_connected() which checks
        existing connection state first.
        """
        logger.info(f'connecting to route: {self.name}, subject: {self.subject}, url: {self.url}')

        try:
            self._nc = NATS()
            await self._nc.connect(servers=[self.url])
            logger.info(f'connected to route: {self.name}, subject: {self.subject}')

            if self.jetstream_enabled:
                await self.create_stream()

            return True
        except Exception as e:
            logger.warning(f"failed to connect to route: {self.name}, subject: {self.subject}: {e}")

        return False

    async def _ensure_connected(self) -> bool:
        """
        Ensure connection is active, connecting only if necessary.
        """
        if self._nc and self._nc.is_connected:
            return True
        return await self.connect()

    async def subscribe_request(self):
        async def callback(msg: Msg):
            await self.callback(self, msg, msg.data.decode())

        self._nc_sub = await self._nc.subscribe(subject=self.subject, queue=self.queue, cb=callback)

    async def subscribe_pull_jetstream(self):
        """
        Create a JetStream pull subscription supporting both exact subject
        and partitioned subjects ({subject}.>).

        Uses add_consumer + pull_subscribe_bind for multiple filter_subjects support.
        """
        durable_name = f"{self.name}_sub_{self.consumer_id}" if self.consumer_id else self.name

        # Create consumer with multiple filter subjects
        try:
            await self._js.add_consumer(
                stream=self.name,
                config=ConsumerConfig(
                    durable_name=durable_name,
                    ack_wait=self.ack_wait,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    max_ack_pending=1000,
                    flow_control=False,
                    filter_subjects=[self.subject, f"{self.subject}.>"],
                ),
            )
        except Exception as e:
            # Consumer may already exist
            logger.debug(f"consumer may already exist: {e}")

        # Bind to the consumer
        self._js_pull_sub = await self._js.pull_subscribe_bind(
            consumer=durable_name,
            stream=self.name,
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

    async def _fetch_messages(self, timeout: float) -> list:
        """
        Fetch messages from JetStream or standard NATS.

        Always returns a list for consistent downstream handling.
        Raises timeout exceptions when no messages are available.
        """
        if self.jetstream_enabled:
            messages = await self._js_pull_sub.fetch(batch=self.batch_size, timeout=timeout)
            if not messages:
                raise nats.js.errors.FetchTimeoutError("no data received")
            return messages if isinstance(messages, list) else [messages]
        else:
            msg = await self._nc.request(self.subject, b'', timeout=timeout)
            if not msg:
                raise nats.aio.errors.ErrTimeout("no data received")
            return [msg]

    async def _process_message(self, msg) -> None:
        """
        Process a single message: log redelivery warnings and invoke callback.
        """
        if self.jetstream_enabled and hasattr(msg, 'metadata') and msg.metadata.num_delivered > 1:
            logger.warning(
                f"Message redelivered {msg.metadata.num_delivered} times "
                f"on subject: {self.subject}, consumer: {msg.metadata.consumer}"
            )
        await self.callback(self, msg, msg.data.decode("utf-8"))

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
            await self._ensure_connected()
            res = await self._nc.request(self.subject, msg, timeout=10.0)
            return res
        except (ErrConnectionClosed, ErrTimeout, ErrNoServers, Exception) as e:
            logger.error(f"failed to request-reply message: {e}")

        return None

    async def reply(self, msg: Any, reply: str) -> None:
        if not isinstance(msg, Msg):
            raise ValueError(f"invalid msg type received, expected nats.aio.Msg got {type(msg)}")

        await msg.respond(reply)

    async def publish(self, msg: Any, subject: str = None) -> Optional[RouteMessageStatus]:
        """
        Publish a message to the route's subject or a custom subject.

        Args:
            msg: Message to publish (str, dict, or bytes)
            subject: Optional custom subject. If not provided, uses route's default subject.
        """
        if not msg:
            return None

        if isinstance(msg, str):
            msg = msg.encode('utf-8')
        elif isinstance(msg, dict):
            msg = json.dumps(msg).encode('utf-8')
        else:
            raise ValueError("Unsupported message type")

        target_subject = subject or self.subject

        try:
            await self._ensure_connected()

            if self.jetstream_enabled:
                logger.debug(f'publishing to route: {self.name}, subject: {target_subject}')
                awk = await self._js.publish(subject=target_subject, payload=msg)
            else:
                await self._nc.publish(subject=target_subject, payload=msg)
                awk = "N/A"

            return RouteMessageStatus(
                id=str(awk),
                status=MessageStatus.QUEUED
            )
        except (ErrConnectionClosed, ErrTimeout, ErrNoServers, Exception) as e:
            logger.error(f"failed to publish message: {e}")
            return RouteMessageStatus(
                message=msg,
                status=MessageStatus.FAILED,
                error=str(e)
            )

    async def consume(self, wait: bool = True):
        logger.info(f'consume:start for route: {self.name}, subject: {self.subject}, js: {self.jetstream_enabled}')

        backoff_base = 0.1
        backoff_factor = 2
        max_backoff = 1.0
        backoff_time = backoff_base
        self.consumer_active = True

        while wait and self.consumer_active:
            try:
                logger.debug(f"pulling messages from subject: {self.subject}, consumer: {self.consumer_id}, batch_size: {self.batch_size}")
                messages = await self._fetch_messages(timeout=backoff_time)

                logger.info(f"received {len(messages)} messages on subject: {self.subject}, consumer: {self.consumer_id}")
                for msg in messages:
                    await self._process_message(msg)

                backoff_time = backoff_base
            except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
                raise InterruptedError(e)
            except (nats.js.errors.FetchTimeoutError, nats.aio.errors.ErrTimeout, TimeoutError):
                logger.debug(f"no data received, backing off for {backoff_time} seconds...")
                if not wait:
                    break
                backoff_time = min(backoff_time * backoff_factor, max_backoff)
            except ValueError as e:
                logger.critical(f"failed to process message, ignoring: {e}")
            except Exception as e2:
                if self.consumer_active:
                    raise ValueError(e2)

        self.consumer_active = False

    async def ack(self, message):
        # TODO should probably check durability rather then jetstream? kind of confusing but yeah. will figure this out at some point
        if self.jetstream_enabled:
            if message:
                logger.debug(f"ack message: {message}")
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
        self.consumer_active = False

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
