import yaml
from typing import Any, Optional

from .base_message_provider import BaseMessagingProducerProvider
from .base_message_route_model import RouteMessageStatus, Route


def get_message_config_from_yaml(yaml_file: str):
    with open(yaml_file, 'r') as file:
        message_config = yaml.safe_load(file)
        return message_config['messageConfig']


class Router:

    def __init__(self, provider: BaseMessagingProducerProvider, yaml_file: str):
        self.provider = provider
        self.yaml_file = yaml_file
        self.message_config = get_message_config_from_yaml(yaml_file=yaml_file)

        # if there is a service base url
        self.root_route = self.provider.create_route(
            route_config=self.message_config['root_route']
        ) if 'root_route' not in self.message_config else None

        self.topic_routes = {}

        # if there are topic selectors then build a dictionary of selector path to the route
        if 'topic_routes' in self.message_config:
            routes = self.message_config['topic_routes']
            self.topic_routes = {
                route_config['selector']:
                    self.provider.create_route(route_config=route_config)
                for route_config in routes
            }

    def send_message(self, selector: str, msg: Any) -> RouteMessageStatus:
        route = self.find_router(selector=selector)

        if not route:
            raise LookupError(f'unable to find topic route: {selector}, nor is a root route defined')

        return route.send_message(msg)

    def find_router(self, selector) -> Optional[Route]:
        if selector not in self.topic_routes:
            return self.root_route

        return self.topic_routes[selector]
