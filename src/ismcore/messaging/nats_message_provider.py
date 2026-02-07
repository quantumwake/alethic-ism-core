from ismcore.messaging.base_message_provider import BaseRouteProvider
from ismcore.messaging.nats_message_route_concurrent import NATSRouteConcurrent
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

        # common route parameters
        route_params = dict(
            name=route_config['name'],
            selector=route_config['selector'],
            url=route_config['url'],
            subject=route_config['subject'],
            ack_wait=route_config.get('ack_wait', 90),
            batch_size=route_config.get('batch_size', 1),
            queue=route_config.get('queue'),
            jetstream_enabled=route_config.get('jetstream_enabled', True),
            concurrent_enabled=route_config.get('concurrent_enabled', False),
            concurrent_max_workers=route_config.get('concurrent_max_workers', 10),
            concurrent_priority_enabled=route_config.get('concurrent_priority_enabled', False),
            concurrent_max_workers_high=route_config.get('concurrent_max_workers_high', 5),
        )

        # use concurrent route if enabled
        if route_params['concurrent_enabled']:
            route = NATSRouteConcurrent(**route_params)
            logger.debug(f"created concurrent route: {route.name}; subject: {route.subject}; workers: {route.concurrent_max_workers}; priority: {route.concurrent_priority_enabled}")
        else:
            route = NATSRoute(**route_params)
            logger.debug(f"created route: {route.name}; selector {route.selector}; subject: {route.subject}; batch: {route.batch_size}")

        return route


