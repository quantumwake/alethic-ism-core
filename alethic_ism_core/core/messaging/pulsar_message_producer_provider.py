from ..messaging.base_message_provider import BaseMessagingProducerProvider
from ..messaging.base_message_route_model import Route
from .pulsar_message_producer_route import PulsarRoute


class PulsarMessagingProducerProvider(BaseMessagingProducerProvider):

    def create_route(self, route_config: dict) -> Route:

        if 'route_config' not in route_config:
            raise KeyError(f'route_config not found in routing file yaml, with data {data}')

        pulsar_route = PulsarRoute(
            route_config=route_config
        )

        return pulsar_route
