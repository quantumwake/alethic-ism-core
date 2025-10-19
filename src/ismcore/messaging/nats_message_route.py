import asyncio
import json
import time
from typing import Any, Optional, Union, Dict, Callable

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

    # concurrent subject processing
    concurrent_subjects: Optional[bool] = False  # enable concurrent processing per actual subject
    max_concurrent_subjects: Optional[int] = None  # max subjects to process concurrently (None = unlimited)

    jetstream_enabled: Optional[bool] = True

    # internal tracking for consumers, as each consumer needs to be unique
    consumer_id: Optional[str] = "1"  # a number to identify the subscriber index on the queue

    # internal objects handling the publishing / subscriber model
    _nc: NATS = PrivateAttr(default=None)  # connection
    _js: JetStreamContext = PrivateAttr(default=None)  # jetstream recv/send

    _nc_sub: nats.aio.client.Subscription = PrivateAttr(default=None)  # subscriber for a regular NATS consumer
    _js_pull_sub: JetStreamContext.PullSubscription = PrivateAttr(default=None)  # sub for pull Jetstream consumer

    # tracking for concurrent subject processing
    _subject_tasks: Dict[str, asyncio.Task] = PrivateAttr(default_factory=dict)  # active tasks per subject
    _subject_tasks_lock: asyncio.Lock = PrivateAttr(default_factory=asyncio.Lock)  # lock for thread-safe access

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
                retention=RetentionPolicy.WORK_QUEUE
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

        self._nc_sub = await self._nc.subscribe(subject=self.subject, queue=self.queue, cb=callback)

    async def subscribe_pull_jetstream(self):
        if self.consumer_id:
            durable_name = f"{self.name}_sub_{self.consumer_id}"
        else:
            durable_name = self.name

        self._js_pull_sub = await self._js.pull_subscribe(
            subject=self.subject,
            durable=durable_name,
            config=ConsumerConfig(
                ack_wait=self.ack_wait,
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

    async def publish(self, msg: Any) -> Optional[RouteMessageStatus]:
        return await self.publish_with_subject(self.subject, msg)

    async def publish_with_subject(self, subject: str, msg: Any) -> Optional[RouteMessageStatus]:
        if not msg:
            return None

        if isinstance(msg, str):
            msg = msg.encode('utf-8')
        elif isinstance(msg, dict):
            msg = json.dumps(msg).encode('utf-8')
        elif isinstance(msg, bytes):
            msg = str(msg).encode('utf-8')
        else:
            raise ValueError("Unsupported message type")

        try:
            await self.connect()

            if self.jetstream_enabled:
                logger.debug(f'preparing to publish data onto jetstream route: {self.name}, subject: {subject}')
                awk = await self._js.publish(subject=subject, payload=msg)
            else:
                logger.debug(f'preparing to publish data onto nats route: {self.name}, subject: {subject}')
                await self._nc.publish(subject=subject, payload=msg)
                awk = "N/A"

            return RouteMessageStatus(id=str(awk), status=MessageStatus.QUEUED)
        except (ErrConnectionClosed, ErrTimeout, ErrNoServers, Exception) as e:
            print("Failed to send message:", e)
            return RouteMessageStatus(
                message=msg,
                status=MessageStatus.FAILED,
                error=str(e)
            )
        finally:
            pass

    async def _fetch_messages(self, timeout: float):
        """
        Fetch messages from NATS (JetStream or standard).

        Args:
            timeout: Timeout duration for fetching messages

        Returns:
            Single message or list of messages

        Raises:
            FetchTimeoutError: If no messages available within timeout
        """
        if self.jetstream_enabled:
            logger.info(f"pulling messages from subject: {self.subject}, consumer: {self.consumer_id}, batch_size: {self.batch_size}")
            msg = await self._js_pull_sub.fetch(batch=self.batch_size, timeout=timeout)
            if not msg:
                raise nats.js.errors.FetchTimeoutError("no data received")
        else:
            msg = await self._nc.request(self.subject, b'', timeout=timeout)
            if not msg:
                raise nats.aio.errors.ErrTimeout("no data received")

        return msg

    def _log_redelivery_warning(self, msg: Msg):
        """
        Log warning if message has been redelivered (JetStream only).

        Args:
            msg: The NATS message to check
        """
        if self.jetstream_enabled and hasattr(msg, 'metadata') and msg.metadata.num_delivered > 1:
            logger.warning(
                f"Message redelivered {msg.metadata.num_delivered} times "
                f"on subject: {self.subject}, consumer: {msg.metadata.consumer}"
            )

    async def _process_single_message(self, msg: Msg):
        """
        Process a single message: log redelivery warning and call callback.

        Args:
            msg: The NATS message to process
        """
        self._log_redelivery_warning(msg)
        await self.callback(self, msg, msg.data.decode("utf-8"))

    async def _subject_task_wrapper(self, msg: Msg):
        """
        Wrapper for processing a message and cleaning up task tracking.

        Args:
            msg: The NATS message to process
        """
        msg_subject = msg.subject
        try:
            await self._process_single_message(msg)
        except Exception as e:
            logger.error(f"Error processing message on subject {msg_subject}: {e}")
            raise
        finally:
            # Clean up task from tracking dict (thread-safe)
            async with self._subject_tasks_lock:
                if msg_subject in self._subject_tasks:
                    del self._subject_tasks[msg_subject]
                    logger.debug(f"Cleaned up task for subject: {msg_subject}")

    async def _spawn_subject_task(self, msg: Msg):
        """
        Spawn an async task for processing this message (thread not safe at this stage).

        Atomically checks capacity and adds task to prevent race conditions.

        Args:
            msg: The NATS message to process

        Returns:
            The spawned task, or None if limit reached
        """
        msg_subject = msg.subject
        task = asyncio.create_task(self._subject_task_wrapper(msg))
        self._subject_tasks[msg_subject] = task
        logger.debug(f"Spawned task for subject: {msg_subject}")
        return task

        # Atomically check capacity and add task in single critical section
        # async with self._subject_tasks_lock:
            # Check limit
            # if self.max_concurrent_subjects is not None:
            #     active_count = len(self._subject_tasks)
            #     if active_count >= self.max_concurrent_subjects:
            #         logger.warning(
            #             f"Max concurrent subjects limit ({self.max_concurrent_subjects}) reached. "
            #             f"Active subjects: {list(self._subject_tasks.keys())}"
            #         )
            #         return None



    async def _process_messages(self, msg: Union[Msg, list[Msg]]):
        """
        Process single message or batch of messages.

        When concurrent_subjects is enabled, spawns tasks per unique subject for parallel processing.
        Otherwise processes sequentially.

        Args:
            msg: Single message or list of messages to process
        """
        messages = msg if isinstance(msg, list) else [msg]

        if isinstance(msg, list):
            logger.info(f"received {len(messages)} messages on subject: {self.subject}, consumer: {self.consumer_id}")

        if self.concurrent_subjects:
            # Concurrent mode: spawn tasks for each message
            for m in messages:
                task = await self._spawn_subject_task(m)
                # Note: If task is None, the consume loop's capacity check should have prevented this
                # But if we somehow got here, the task will handle itself or we just don't process it
        else:
            # Sequential mode: process each message inline
            for m in messages:
                await self._process_single_message(m)

    async def check_and_process(self, callback: Callable):
        # If concurrent mode with limit, check capacity before fetching
        if not self.concurrent_subjects or not self.max_concurrent_subjects:
            await callback() # No limit, proceed
            return

        # concurrent mode with limit - check capacity
        async with self._subject_tasks_lock:
            current_count = len(self._subject_tasks)

            if current_count >= self.max_concurrent_subjects:
                # At capacity, wait before retrying
                await asyncio.sleep(0.1)
                return # Skip processing this cycle

            await callback()

    async def consume(self, wait: bool = True):
        """
        Consume messages from NATS with exponential backoff retry logic.

        Args:
            wait: If True, continuously poll for messages. If False, fetch once and exit.
        """
        logger.info(f'consume:start for route: {self.name}, subject: {self.subject}, js: {self.jetstream_enabled}')

        # Backoff parameters
        backoff_base = 0.1  # Starting backoff time in seconds
        backoff_factor = 2  # Exponential backoff factor
        max_backoff = 1     # Maximum backoff time in seconds
        backoff_time = backoff_base

        self.consumer_active = True

        async def process_messages_callback(timeout: float):
            msg = await self._fetch_messages(timeout=timeout)
            await self._process_messages(msg)
            return msg

        while wait and self.consumer_active:
            try:
                await self.check_and_process(
                    lambda: process_messages_callback(timeout=backoff_time)
                )

                # Reset backoff time on success
                backoff_time = backoff_base
                time.sleep(0.1)

            except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
                raise InterruptedError(e)

            except (nats.js.errors.FetchTimeoutError, nats.aio.errors.ErrTimeout, TimeoutError):
                logger.info(f"no data received, backing off for {backoff_time} seconds...")

                if not wait:
                    break

                # Increase backoff time exponentially
                backoff_time = min(backoff_time * backoff_factor, max_backoff)

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

    async def nak(self, message, delay: Optional[float] = None):
        """
        Negatively acknowledge a message (JetStream only).

        Args:
            message: The NATS message to NAK
            delay: Optional delay in seconds before redelivery

        Returns:
            True if message was nacked, False otherwise
        """
        if self.jetstream_enabled:
            if message:
                logger.debug(f"nak message: {message}")
                if delay:
                    await message.nak(delay=delay)
                else:
                    await message.nak()
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
