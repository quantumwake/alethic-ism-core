import json

from alethic_ism_core.core.base_message_route_model import MessageStatus
from alethic_ism_core.core.base_message_router import Router
from alethic_ism_core.core.pulsar_message_producer_provider import PulsarMessagingProducerProvider


def test_pulsar_route_1():
    provider = PulsarMessagingProducerProvider()
    router = Router(provider=provider, yaml_file="./test_routes/test_pulsar_route.yaml")
    route = router.find_router(selector="test/topic")
    status = route.send_message(msg=json.dumps({
        "name": "hello",
        "hello": "world",
        "world": "goodbye"
    }))
    assert route is not None
    assert status.status == MessageStatus.QUEUED
    assert route.schema == "schema.StringSchema"




