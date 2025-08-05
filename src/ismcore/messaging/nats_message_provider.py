from ismcore.messaging.base_message_provider import BaseRouteProvider
from ismcore.messaging.nats_message_route import NATSRoute
from ismcore.utils.ism_logger import ism_logger

logger = ism_logger(__name__)

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

        route = NATSRoute(
            name=route_config['name'],
            selector=route_config['selector'],
            url=route_config['url'],
            subject=route_config['subject'],
            ack_wait=route_config['ack_wait'] if 'ack_wait' in route_config else 90,
            batch_size=route_config['batch_size'] if 'batch_size' in route_config else 1,
            queue=route_config['queue'] if 'queue' in route_config else None, ### TODO - queue is not in the yaml file? maybe not needed for pull subscriptions but needed for requests?
            jetstream_enabled=route_config['jetstream_enabled'] if 'jetstream_enabled' in route_config else True,
            # group=route_config['group'] if 'group' in route_config else None,
        )
        logger.debug(f"created route: {route.name}; selector {route.selector}; subject: {route.subject}; batch: {route.batch_size}; ack_wait: {route.ack_wait}; queue: {route.queue}; jetstream_enabled: {route.jetstream_enabled}")
        return route


