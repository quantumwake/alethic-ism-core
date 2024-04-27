import logging
from enum import Enum

import pulsar
import yaml

from typing import Union, Optional, Any
from pulsar.schema import schema
from pydantic import BaseModel


class MessageStatus(Enum):
    QUEUED = "QUEUED"
    FAILED = "FAILED"


class RouteMessageStatus(BaseModel):
    id: Optional[str] = None
    message: Optional[Any] = None
    status: MessageStatus
    error: Optional[str] = None


class Route:

    client: Union[pulsar.Client]
    producer_topic: Union[pulsar.Producer, pulsar.Consumer]
    producer_manage: Union[pulsar.Producer, pulsar.Consumer]

    def __init__(self, topic_selector: dict, root_service_url: str = None):

        # routing configuration
        self.selector = topic_selector['selector'] \
            if 'selector' in topic_selector \
            else None

        self.topic = topic_selector['topic']
        self.manage_topic = topic_selector['manage_topic'] \
            if 'manage' in topic_selector \
            else None

        self.subscription = topic_selector['subscription']
        self.service_url = topic_selector['service_url'] \
            if 'service_url' in topic_selector \
            else root_service_url

        # raise exception if service url is not defined
        if not self.service_url:
            raise ConnectionError('service url is not defined in messageConfig or in the specific topic selector, '
                                  'either specify the top level service url or a topic level service url')

        # routing client used to actual produce/consume data
        self.client = pulsar.Client(self.service_url)
        self.producer_topic = self.client.create_producer(
            self.topic,
            schema=schema.StringSchema()
        )

        self.producer_manage = self.client.create_producer(
            self.manage_topic,
            schema=schema.StringSchema()
        ) if self.manage_topic else None

    def send_message(self, msg) -> RouteMessageStatus:
        try:
            id = self.producer_topic.send(msg, None)
            return RouteMessageStatus(
                id=str(id),
                status=MessageStatus.QUEUED
            )
        except Exception as e:
            print("Failed to send message: %s", e)
            return RouteMessageStatus(
                message=msg,
                status=MessageStatus.FAILED,
                error=str(e)
            )
            # raise e
        finally:
            try:
                self.producer_topic.flush()
            except:
                pass


def get_message_config_from_yaml(yaml_file: str):
    with open(yaml_file, 'r') as file:
        message_config = yaml.safe_load(file)
        return message_config['messageConfig']


class Router:

    def __init__(self, yaml_file: str):
        self.yaml_file = yaml_file
        self.message_config = get_message_config_from_yaml(yaml_file=yaml_file)

        # if there is a service base url
        self.root_route = Route(topic_selector=self.message_config['root_route']) \
            if 'root_route' in self.message_config \
            else None

        # if there are topic selectors then build a dictionary of selector path to the route
        self.topic_routes = {
            topic_selector['selector']: Route(
                topic_selector=topic_selector,
                root_service_url=self.root_route.service_url)
            for topic_selector in self.message_config['topic_routes']
        } if 'topic_routes' in self.message_config \
            else None

    def send_message(self, selector: str, msg: dict) -> RouteMessageStatus:
        route = self.find_router(selector=selector)

        if not route:
            raise LookupError(f'unable to find topic route: {selector}, nor is a root route defined')

        return route.send_message(msg)

    def find_router(self, selector) -> Optional[Route]:
        if selector not in self.topic_routes:
            return self.root_route

        return self.topic_routes[selector]

    # def get_message_config_url(self, selector: str = None) -> str:
    #     config = self.message_config
    #
    #     url = config['url'] if 'url' in config else None
    #
    #     # iterate each topic and check for specific urls, if any
    #     # _topics = self.topics
    #
    #     # check if url is strictly specified in the route
    #     specific_url = [
    #         topic['url'] for topic in _topics
    #         if 'url' in topic
    #            and selector in topic['selector']
    #     ]
    #
    #     return specific_url[0] if specific_url else url

    #
    # def get_message_topics(self):
    #     return self.message_config['topics']

    # def get_message_topic(self, topic: str):
    #     topics = self.routes
    #     topic = topics[topic] if topic in topics else None
    #
    #     if topic:
    #         return topic
    #
    #     logging.error(f'invalid topic name: {topic} requested from topics: {topics}')
    #     return None

    # def get_message_routes(self, selector: str = None):
    #     # global routes
    #
    #     if not self.routes:
    #         routes = {
    #             topic['selector']: Route(topic)
    #             for topic in self.message_topics
    #         }
    #
    #     if selector:
    #         return routes[selector]
    #     else:
    #         return routes
    #
    # def get_route_by_processor(self, processor_id: str) -> Route:
    #     available_routes = self.message_routes
    #     if processor_id not in available_routes:
    #         raise NotImplementedError(f'message routing is not defined for processor state id: {processor_id}, '
    #                                   f'please make sure to setup a route selector as part of the routing.yaml')
    #
    #     return routes[processor_id]


