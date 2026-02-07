import asyncio
from typing import Any, Set
from pydantic import PrivateAttr
from nats.js.api import ConsumerConfig, DeliverPolicy, AckPolicy
from ismcore.messaging.nats_message_route import NATSRoute
from ismcore.utils.ism_logger import ism_logger

logger = ism_logger(__name__)


class NATSRouteConcurrent(NATSRoute):
    """
    Concurrent NATS route with optional priority queue support.

    Extends NATSRoute to provide:
    - Semaphore-based concurrent message handling (goroutine-style)
    - Optional separate high/low priority subscriptions

    When concurrent_priority_enabled=True:
        Subscribes to {subject}.high.* and {subject}.low.*
        Each priority level has its own semaphore pool

    When concurrent_priority_enabled=False:
        Uses standard subscription with concurrent dispatch
    """

    # concurrent mode internals
    _js_pull_sub_high: Any = PrivateAttr(default=None)
    _js_pull_sub_low: Any = PrivateAttr(default=None)
    _semaphore: asyncio.Semaphore = PrivateAttr(default=None)
    _semaphore_high: asyncio.Semaphore = PrivateAttr(default=None)
    _pending_tasks: Set[asyncio.Task] = PrivateAttr(default_factory=set)

    async def subscribe(self) -> bool:
        """Subscribe with priority support if enabled."""
        logger.info(f'subscribe:start to route: {self.name}, subject: {self.subject}, priority: {self.concurrent_priority_enabled}')

        await self._ensure_connected()

        if self.concurrent_priority_enabled:
            await self._subscribe_priority()
        else:
            await self.subscribe_pull_jetstream()
            self._semaphore = asyncio.Semaphore(self.concurrent_max_workers or 10)

        logger.info(f'subscribe:complete to route: {self.name}, subject: {self.subject}')
        return True

    async def _subscribe_priority(self):
        """
        Create separate pull subscriptions for high and low priority subjects.

        Subject patterns:
            High priority pool:
                {base_subject}.high            → high priority (exact)
                {base_subject}.high.>          → high priority (with partition)

            Low priority pool (includes fallback):
                {base_subject}                 → base subject (fallback for non-priority publishers)
                {base_subject}.low             → low priority (exact)
                {base_subject}.low.>           → low priority (with partition)

        Uses add_consumer + pull_subscribe_bind for multiple filter_subjects support.
        """
        durable_high = f"{self.name}_high_{self.consumer_id}"
        durable_low = f"{self.name}_low_{self.consumer_id}"

        # Create high priority consumer
        try:
            await self._js.add_consumer(
                stream=self.name,
                config=ConsumerConfig(
                    durable_name=durable_high,
                    ack_wait=self.ack_wait,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    max_ack_pending=1000,
                    flow_control=False,
                    filter_subjects=[f"{self.subject}.high", f"{self.subject}.high.>"],
                ),
            )
        except Exception as e:
            logger.debug(f"high priority consumer may already exist: {e}")

        self._js_pull_sub_high = await self._js.pull_subscribe_bind(
            consumer=durable_high,
            stream=self.name,
        )
        logger.info(f"subscribed to high priority: {self.subject}.high[.>]")

        # Create low priority consumer (includes base subject fallback)
        try:
            await self._js.add_consumer(
                stream=self.name,
                config=ConsumerConfig(
                    durable_name=durable_low,
                    ack_wait=self.ack_wait,
                    deliver_policy=DeliverPolicy.ALL,
                    ack_policy=AckPolicy.EXPLICIT,
                    max_ack_pending=1000,
                    flow_control=False,
                    filter_subjects=[self.subject, f"{self.subject}.low", f"{self.subject}.low.>"],
                ),
            )
        except Exception as e:
            logger.debug(f"low priority consumer may already exist: {e}")

        self._js_pull_sub_low = await self._js.pull_subscribe_bind(
            consumer=durable_low,
            stream=self.name,
        )
        logger.info(f"subscribed to low priority: {self.subject}[.low[.>]]")

        # initialize semaphores
        self._semaphore = asyncio.Semaphore(self.concurrent_max_workers or 10)
        self._semaphore_high = asyncio.Semaphore(self.concurrent_max_workers_high or 5)

    def get_publish_subject(self, priority: str = None, partition_key: str = None) -> str:
        """
        Get the subject to publish messages to.

        When priority mode is enabled, constructs subjects like:
            {base_subject}.high
            {base_subject}.low.{partition_key}

        Args:
            priority: "high" or "low". Defaults to "low" when priority is enabled.
            partition_key: Optional partition key (e.g., project_id). Appended if provided.

        Returns:
            The subject string to publish to.
        """
        if not self.concurrent_priority_enabled:
            return self.subject

        priority = priority or "low"
        if priority not in ("high", "low"):
            raise ValueError(f"priority must be 'high' or 'low', got '{priority}'")

        if partition_key:
            return f"{self.subject}.{priority}.{partition_key}"
        return f"{self.subject}.{priority}"

    def get_publish_subject_high(self, partition_key: str = None) -> str:
        """Convenience method for high priority publish subject."""
        return self.get_publish_subject(priority="high", partition_key=partition_key)

    def get_publish_subject_low(self, partition_key: str = None) -> str:
        """Convenience method for low priority publish subject."""
        return self.get_publish_subject(priority="low", partition_key=partition_key)

    def _dispatch(self, msg: Any, semaphore: asyncio.Semaphore, label: str = "default"):
        """
        Fire-and-forget task dispatch with semaphore for concurrency control.

        Note: The callback is responsible for acking the message.
        """
        if hasattr(msg, 'metadata') and msg.metadata.num_delivered > 1:
            logger.warning(f"[{label}] redelivery #{msg.metadata.num_delivered}: {msg.subject}")

        async def handle():
            async with semaphore:
                try:
                    await self.callback(self, msg, msg.data.decode("utf-8"))
                except Exception as e:
                    logger.error(f"[{label}] handler error on subject {msg.subject}: {e}")

        task = asyncio.create_task(handle())
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)

    async def _consume_loop(self, subscription, semaphore: asyncio.Semaphore, label: str, wait: bool = True):
        """Consume loop for a single subscription with concurrent dispatch."""
        from nats.js.errors import FetchTimeoutError

        backoff_base = 0.1
        backoff_factor = 2
        max_backoff = 1.0
        backoff_time = backoff_base

        while wait and self.consumer_active:
            try:
                messages = await subscription.fetch(batch=self.batch_size, timeout=backoff_time)
                if messages:
                    logger.debug(f"[{label}] received {len(messages)} messages")
                    for msg in messages:
                        self._dispatch(msg, semaphore, label)
                    backoff_time = backoff_base
            except (asyncio.TimeoutError, FetchTimeoutError, TimeoutError):
                backoff_time = min(backoff_time * backoff_factor, max_backoff)
            except Exception as e:
                if self.consumer_active:
                    logger.error(f"[{label}] consume error: {e}")
                    backoff_time = min(backoff_time * backoff_factor, max_backoff)

    async def consume(self, wait: bool = True):
        """Consume with concurrent dispatch."""
        logger.info(f'consume:start for route: {self.name}, subject: {self.subject}, priority: {self.concurrent_priority_enabled}')

        self.consumer_active = True

        if self.concurrent_priority_enabled:
            # Priority mode: two parallel consume loops
            logger.info(f"starting parallel consume loops (priority mode)")
            await asyncio.gather(
                self._consume_loop(self._js_pull_sub_high, self._semaphore_high, "high", wait),
                self._consume_loop(self._js_pull_sub_low, self._semaphore, "low", wait),
            )
        else:
            # Non-priority mode: single loop with semaphore dispatch
            logger.info(f"starting consume loop (concurrent mode)")
            await self._consume_loop(self._js_pull_sub, self._semaphore, "default", wait)

        self.consumer_active = False

    async def stop(self, wait_for_pending: bool = True):
        """Stop consuming and optionally wait for pending tasks to complete."""
        logger.info(f"stopping concurrent route, pending tasks: {len(self._pending_tasks)}")
        self.consumer_active = False

        if wait_for_pending and self._pending_tasks:
            await asyncio.gather(*self._pending_tasks, return_exceptions=True)

        logger.info("concurrent route stopped")
