from datetime import datetime as dt
from enum import Enum
from typing import Optional, Any, Callable, Awaitable
from pydantic import BaseModel

class MessageStatus(Enum):
    QUEUED = "QUEUED"
    FAILED = "FAILED"


class RouteMessageStatus(BaseModel):
    id: Optional[str] = None
    message: Optional[Any] = None
    status: MessageStatus
    error: Optional[str] = None


class BaseRoute(BaseModel):

    # route selector
    selector: str

    # callback function when messages arrive
    callback: Optional[Callable[['BaseRoute', Any, Any], Awaitable[None]]] = None

    # consumer is actively consuming data flag
    consumer_active: Optional[bool] = False

    # when the consumer was created
    creation_date: Optional[dt] = dt.utcnow()

    async def connect(self):
        """
        Establish connection to the messaging backend.

        Returns:
            bool: True if connection successful, False otherwise.
        """
        raise NotImplementedError()

    async def _ensure_connected(self) -> bool:
        """
        Ensure connection is active, reconnecting if necessary.

        Unlike connect(), this checks existing connection state first
        and only connects if not already connected.

        Returns:
            bool: True if connected (existing or new), False if connection failed.
        """
        raise NotImplementedError()

    async def disconnect(self):
        """
        Disconnect from the messaging backend.

        Handles cleanup including draining pending messages and closing connections.
        """
        raise NotImplementedError()

    async def subscribe(self):
        """
        Subscribe to the route's subject/topic for receiving messages.

        Creates the appropriate subscription based on route configuration
        (e.g., JetStream pull subscription, standard NATS subscription).

        Returns:
            bool: True if subscription successful.
        """
        raise NotImplementedError()

    async def request(self, msg: Any) -> Any:
        """
        Send a request and wait for a reply (request-reply pattern).

        Args:
            msg: Message payload to send.

        Returns:
            The reply message, or None if request failed/timed out.
        """
        raise NotImplementedError()

    async def reply(self, msg: Any, reply: str) -> None:
        """
        Send a reply to a received message (request-reply pattern).

        Args:
            msg: The original message being replied to.
            reply: The reply payload to send.
        """
        raise NotImplementedError()

    async def publish(self, msg: Any, subject: str = None) -> Optional[RouteMessageStatus]:
        """
        Publish a message to the route.

        Args:
            msg: Message payload to publish (str, dict, or bytes depending on implementation).
            subject: Optional override for the target subject/topic. If not provided,
                     use get_publish_subject() to determine the appropriate subject.

        Returns:
            RouteMessageStatus indicating success/failure and message ID if available.
        """
        raise NotImplementedError()

    def get_publish_subject(self, priority: str = None, partition_key: str = None) -> str:
        """
        Get the subject/topic to publish messages to.

        Implementations should return the appropriate subject based on route configuration.
        For routes with priority support, this constructs the correct subject pattern.

        Subject patterns (implementation-specific):
            - Base: {subject}
            - With priority: {subject}.{priority}
            - With priority + partition: {subject}.{priority}.{partition_key}

        Args:
            priority: Optional priority level (e.g., "high", "low"). Ignored if route
                      does not support priority-based routing.
            partition_key: Optional partition key for message routing/grouping.

        Returns:
            The subject/topic string to publish to.
        """
        raise NotImplementedError()

    async def consume(self, wait: bool = True):
        """
        Start consuming messages from the subscribed subject/topic.

        Runs a consume loop that fetches messages and invokes the callback.
        Implements exponential backoff when no messages are available.

        Args:
            wait: If True, blocks and continuously consumes. If False, returns
                  after processing available messages or on first timeout.
        """
        raise NotImplementedError()

    async def ack(self, message):
        """
        Acknowledge a message as successfully processed.

        For durable subscriptions, this marks the message as consumed
        so it won't be redelivered.

        Args:
            message: The message to acknowledge.

        Returns:
            bool: True if acknowledgment successful.
        """
        raise NotImplementedError()

    async def flush(self):
        """
        Flush any pending published messages to the server.

        Ensures all buffered outgoing messages are sent.
        """
        raise NotImplementedError()

    async def drain(self):
        """
        Drain the connection gracefully.

        Stops accepting new messages, processes pending messages,
        and prepares for disconnect.

        Returns:
            bool: True if drain completed successfully.
        """
        raise NotImplementedError()

    def get_message_id(self, message) -> str:
        """
        Extract a unique identifier from a message.

        Args:
            message: The message to extract ID from.

        Returns:
            str: Unique message identifier.
        """
        raise NotImplementedError()

    def friendly_message(self, message: Any) -> str:
        """
        Get a human-readable representation of a message.

        Useful for logging and debugging.

        Args:
            message: The message to format.

        Returns:
            str: Human-readable message representation.
        """
        raise NotImplementedError()

    def clone(self, route_config_updates: dict) -> 'BaseRoute':
        """
        Create a copy of this route with updated configuration.

        Args:
            route_config_updates: Dictionary of config values to override.

        Returns:
            A new route instance with the updated configuration.
        """
        raise NotImplementedError()

    @property
    def subject_group(self) -> str:
        """
        Get the subject/topic identifier for this route.

        The naming varies by messaging backend:
            - NATS: subject
            - Kafka/Pulsar: topic
            - SQS/Kinesis: queue/stream name

        Returns:
            str: The subject/topic identifier.
        """
        raise NotImplementedError('the subject property must be defined per route class type, '
                                  'in NATS for example its called a subject, in pulsar or kafka a topic, '
                                  'in kinesis or sqs, something else.')

    @property
    def get_selector_name(self) -> str:
        """
        Get the route selector name.

        The selector is used for routing messages to the appropriate handler.

        Returns:
            str: The selector string for this route.
        """
        return self.selector
