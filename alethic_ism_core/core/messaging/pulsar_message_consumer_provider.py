import pulsar
from typing import Any
from .base_message_provider import BaseMessagingConsumerProvider
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class PulsarMessagingConsumerProvider(BaseMessagingConsumerProvider):

    def __init__(self,
                 message_url: str,
                 message_topic,
                 message_topic_subscription):

        self.client = pulsar.Client(message_url)
        self.message_topic = message_topic
        self.message_topic_subscription = message_topic_subscription
        self.main_consumer = self.client.subscribe(message_topic, message_topic_subscription)

    def close(self):
        self.main_consumer.close()

    def wait(self) -> [Any, Any]:
        try:
            msg = self.main_consumer.receive()
            data = msg.data().decode("utf-8")
            return msg, data
        except pulsar.Interrupted as e:
            # important to raise an interrupt error to exit the runloop
            # since base class only supports InterruptedError
            raise InterruptedError(e)

    def ack(self, message):
        if message:
            self.main_consumer.acknowledge(message=message)
        else:
            logging.error(f"message id is not set on main consumer {self.message_topic}")

    def get_message_id(self, message):
        if message:
            return message.message_id()

        return None

    def friendly_message(self, message: Any):
        if not isinstance(message, pulsar.Message):
            raise ValueError(f"unable to processor message of type {type(message) if message else None}")

        return str({
            "message_id": message.message_id(),
            "partition_key": message.partition_key(),
            "ordering_key": message.ordering_key(),
            "topic_name": message.topic_name()
        })
