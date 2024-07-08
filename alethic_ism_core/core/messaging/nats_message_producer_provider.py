from ..messaging.base_message_provider import BaseMessagingProducerProvider
from ..messaging.base_message_route_model import Route
from .nats_message_producer_route import NATSRoute


class NATSMessagingProducerProvider(BaseMessagingProducerProvider):

    def create_route(self, route: dict) -> Route:
        # extract the route information as derived by .routing-nats.yaml
        # messageConfig:
        #     routes:
        #       - name: route_name
        #         url: nats://localhost:61891
        #         queue: "ism.test"
        #         subject: "ism.test"
        #         selector: mock/route/selector/path

        return NATSRoute(
            name=route['name'],
            selector=route['selector'],
            url=route['url'],
            subject=route['subject'],
            queue=route['queue'] if 'queue' in route else None
        )
