from .base_message_provider import BaseRouteProvider
from .nats_message_route import NATSRoute


class NATSMessageProvider(BaseRouteProvider):

    def create_route(self, route_config: dict) -> NATSRoute:
        # extract the route information as derived by .routing-nats.yaml
        # messageConfig:
        #     routes:
        #       - name: route_name
        #         url: nats://localhost:61891
        #         queue: "ism.test"
        #         subject: "ism.test"
        #         stream: "??"
        #         selector: mock/route/selector/path

        return NATSRoute(
            name=route_config['name'],
            selector=route_config['selector'],
            url=route_config['url'],
            subject=route_config['subject'],
            queue=route_config['queue'] if 'queue' in route_config else None,
            group=route_config['group'] if 'group' in route_config else None,
        )


