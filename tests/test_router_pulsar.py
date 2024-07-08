import asyncio
import json

from alethic_ism_core.core.messaging.base_message_route_model import MessageStatus
from alethic_ism_core.core.messaging.base_message_router import Router
from alethic_ism_core.core.messaging.pulsar_message_producer_provider import PulsarMessagingProducerProvider


def test_pulsar_route_1():
    provider = PulsarMessagingProducerProvider()
    router = Router(provider=provider, yaml_file="./test_routes/test_pulsar_route.yaml")
    asyncio.run(router.connect())

    route = router.find_router(selector="test/topic")
    status = asyncio.run(route.send_message(msg=json.dumps({
        "name": "hello",
        "hello": "world",
        "world": "goodbye"
    })))
    assert route is not None
    assert status.status == MessageStatus.QUEUED
    assert route.schema == "schema.StringSchema"




