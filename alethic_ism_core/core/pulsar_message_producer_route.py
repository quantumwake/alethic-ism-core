import pulsar
from typing import Any, Optional
from pulsar.schema import schema
from pydantic import BaseModel, PrivateAttr
from .base_message_route_model import Route, RouteMessageStatus, MessageStatus


class PulsarRoute(Route, BaseModel):

    service_url: str
    subscription: str
    schema: Optional[str] = "schema.StringSchema"

    _client: pulsar.Client = PrivateAttr()
    _producer_topic: Optional[pulsar.Producer] = PrivateAttr(default=None)
    _producer_manage: Optional[pulsar.Producer] = PrivateAttr(default=None)


    # @property
    def get_schema(self):
        if "schema.StringSchema" == self.schema:
            return schema.StringSchema()
        else:
            raise NotImplementedError("only json objects in the format of a string is supported, "
                                      "you must pass a dictionary as a string type")

    def __init__(self, **data):

        if 'route_config' in data:
            rc = data['route_config']
            data = {
                **data,
                'schema': rc['schema'] if 'schema' in rc else "schema.StringSchema",
                'subscription': rc['subscription'],
                'service_url': rc['service_url'],
            }

            super().__init__(**data)

        # routing client used to actual produce/consume data
        self._client = pulsar.Client(self.service_url)
        self._producer_topic = self._client.create_producer(
            self.topic,
            schema=self.get_schema()
        )

        self._producer_manage = self._client.create_producer(
            self.manage_topic,
            schema=self.get_schema()
        ) if self.manage_topic else None

    def send_message(self, msg: Any) -> RouteMessageStatus:
        try:
            msg_id = self._producer_topic.send(msg, None)
            return RouteMessageStatus(
                id=str(msg_id),
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

