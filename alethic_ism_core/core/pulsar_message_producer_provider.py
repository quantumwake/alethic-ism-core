from .base_message_provider import BaseMessagingProducerProvider
from .base_message_route_model import Route
from .pulsar_message_producer_route import PulsarRoute


class PulsarMessagingProducerProvider(BaseMessagingProducerProvider):

    def create_route(self, route_config: dict) -> Route:

        pulsar_route = PulsarRoute(
            route_config=route_config
        )

        return pulsar_route
