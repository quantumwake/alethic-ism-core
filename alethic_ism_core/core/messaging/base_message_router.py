import json

import yaml
from typing import Any, Optional, Dict

from .base_message_provider import BaseRouteProvider
from .base_message_route_model import RouteMessageStatus, BaseRoute
from ..errors import RouteNotFoundError
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)


def get_message_config_from_yaml(yaml_file: str):
    with open(yaml_file, 'r') as file:
        message_config = yaml.safe_load(file)
        return message_config['messageConfig']


class Router:

    def __init__(self, provider: BaseRouteProvider, yaml_file: str):
        self.provider = provider
        self.yaml_file = yaml_file
        self.message_config = get_message_config_from_yaml(yaml_file=yaml_file)

        self.routing_table = Dict[str, BaseRoute]

        # if there are topic selectors then build a dictionary of selector path to the route
        if 'routes' in self.message_config:
            routes = self.message_config['routes']
            self.routing_table = {
                route_config['selector']:
                    self.provider.create_route(route_config=route_config)
                for route_config in routes
            }

    async def connect_all(self):
        if not self.routing_table:
            raise RouteNotFoundError(
                f'no routes defined, cannot establish connection to any route with no routes defined')

        logging.info(f"connecting to {len(self.routing_table)} routes")
        for _, route in self.routing_table.items():
            await route.connect()

    async def connect(self, selector: str):
        route = self.find_route(selector=selector)
        await route.connect()

    async def disconnect(self):
        if self.routing_table:
            for _, route in self.routing_table.items():
                await route.disconnect()
                logging.info(f"disconnecting to route {route.get_topic}")

    async def publish(self, selector: str, msg: Any) -> RouteMessageStatus:
        route = self.find_route(selector=selector)
        return route.publish(msg)

    async def subscribe_all(self, selector: str):
        if not self.routing_table:
            raise RouteNotFoundError(
                f'no routes defined, cannot establish connection to any route with no routes defined')

        logging.info(f"connecting to {len(self.routing_table)} routes")
        for _, route in self.routing_table.items():
            await route.subscribe()

    async def subscribe(self, selector: str):
        route = self.find_route(selector=selector)
        await route.subscribe()

    def find_route_by_subject(self, subject: str):
        routes = [
            route for selector, route in self.routing_table.items()
            if route.subject_group == subject
        ]

        if routes and len(routes) >= 1:
            first_route = routes[0]
            if not isinstance(first_route, BaseRoute):
                raise TypeError(f"invalid type for routes {type(first_route)}")

            route_config = json.loads(first_route.model_dump_json())
            return self.provider.create_route(
                route_config={**route_config, **{"selector": f"subject group: {subject}"}}
            )

        return None

    def find_route(self, selector) -> Optional[BaseRoute]:
        if selector not in self.routing_table:
            raise LookupError(f"unable to find route selector {selector}")

        return self.routing_table[selector]
