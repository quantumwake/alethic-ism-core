import json
from collections import defaultdict
from typing import Optional, Callable

import nats.aio.errors
import nats.js.errors
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from ismcore.messaging.nats_message_route import NATSRoute
from ismcore.utils.ism_logger import ism_logger

logger = ism_logger(__name__)


class NATSRouteBatch(NATSRoute):
    """
    Batch-aware NATS route that fetches multiple messages, groups them
    by a configurable key, and delivers grouped batches to a callback.
    """
    batch_callback: Optional[Callable] = None
    group_by_fn: Optional[Callable] = None

    @classmethod
    def from_route(cls, route: NATSRoute,
                   batch_callback: Callable, group_by_fn: Callable) -> 'NATSRouteBatch':
        """Construct a NATSRouteBatch from an existing NATSRoute's config."""
        return cls(
            **route.model_dump(),
            batch_callback=batch_callback,
            group_by_fn=group_by_fn,
        )

    async def consume(self, wait: bool = True):
        logger.info(
            f'consume:start (batch) for route: {self.name}, '
            f'subject: {self.subject}, batch_size: {self.batch_size}'
        )

        backoff_base = 0.1
        backoff_factor = 2
        max_backoff = 1.0
        backoff_time = backoff_base
        self.consumer_active = True

        while wait and self.consumer_active:
            try:
                messages = await self._fetch_messages(timeout=backoff_time)

                logger.info(
                    f"fetched {len(messages)} messages on subject: {self.subject}"
                )

                # Parse all messages into (nats_msg, parsed_dict) pairs
                parsed = []
                unparseable = []
                for msg in messages:
                    try:
                        data = json.loads(msg.data.decode('utf-8'))
                        parsed.append((msg, data))
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        logger.warning(f"skipping unparseable message: {e}")
                        unparseable.append(msg)

                # Group by key function
                groups = defaultdict(list)
                ungrouped_msgs = []
                for nats_msg, data in parsed:
                    try:
                        key = self.group_by_fn(data) if self.group_by_fn else None
                        if key is None:
                            logger.warning("message has no group key, skipping")
                            ungrouped_msgs.append(nats_msg)
                            continue
                        groups[key].append(data)
                    except Exception as e:
                        logger.warning(f"failed to extract group key: {e}")
                        ungrouped_msgs.append(nats_msg)

                logger.info(
                    f"batch grouped into {len(groups)} groups "
                    f"({sum(len(v) for v in groups.values())} messages)"
                )

                # Process each group via batch_callback
                for group_key, group_messages in groups.items():
                    try:
                        await self.batch_callback(self, group_key, group_messages)
                    except Exception as e:
                        logger.error(
                            f"error processing batch for group {group_key}: {e}"
                        )

                # Deferred ack: ack all messages after all groups processed
                for msg in messages:
                    try:
                        await self.ack(msg)
                    except Exception as e:
                        logger.warning(f"failed to ack message: {e}")

                backoff_time = backoff_base
            except (ErrConnectionClosed, ErrTimeout, ErrNoServers) as e:
                raise InterruptedError(e)
            except (nats.js.errors.FetchTimeoutError, nats.aio.errors.ErrTimeout, TimeoutError):
                logger.debug(f"no data received, backing off for {backoff_time} seconds...")
                if not wait:
                    break
                backoff_time = min(backoff_time * backoff_factor, max_backoff)
            except ValueError as e:
                logger.critical(f"failed to process batch, ignoring: {e}")
            except Exception as e:
                if self.consumer_active:
                    raise ValueError(e)

        self.consumer_active = False
