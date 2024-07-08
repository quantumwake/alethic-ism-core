import yaml
from typing import Any, Optional

from .base_message_provider import BaseMessagingProducerProvider
from .base_message_route_model import RouteMessageStatus, Route
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


def get_message_config_from_yaml(yaml_file: str):
    with open(yaml_file, 'r') as file:
        message_config = yaml.safe_load(file)
        return message_config['messageConfig']


class Router:

    def __init__(self, provider: BaseMessagingProducerProvider, yaml_file: str):
        self.provider = provider
        self.yaml_file = yaml_file
        self.message_config = get_message_config_from_yaml(yaml_file=yaml_file)

        self.routing_table = {}

        # if there are topic selectors then build a dictionary of selector path to the route
        if 'routes' in self.message_config:
            routes = self.message_config['routes']
            self.routing_table = {
                route_config['selector']:
                    self.provider.create_route(route=route_config)
                for route_config in routes
            }

    async def connect(self):
        if self.routing_table:
            logging.info(f"connecting to route")

            for _, route in self.routing_table.items():
                await route.connect()

    async def disconnect(self):
        if self.routing_table:
            for _, route in self.routing_table.items():
                await route.disconnect()
                logging.info(f"disconnecting to route {route.get_topic}")

    def send_message(self, selector: str, msg: Any) -> RouteMessageStatus:
        route = self.find_router(selector=selector)
        return route.send_message(msg)

    def find_router(self, selector) -> Optional[Route]:
        if selector not in self.routing_table:
            raise LookupError(f"unable to find route selector {selector}")

        return self.routing_table[selector]
