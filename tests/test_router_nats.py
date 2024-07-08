import json
import asyncio
from concurrent.futures import ThreadPoolExecutor

from alethic_ism_core.core.messaging.base_message_route_model import MessageStatus
from alethic_ism_core.core.messaging.base_message_router import Router
from alethic_ism_core.core.messaging.nats_message_consumer_provider import NATSMessagingConsumerProvider
from alethic_ism_core.core.messaging.nats_message_producer_provider import NATSMessagingProducerProvider

import pytest
from alethic_ism_core.core.messaging.nats_message_producer_route import NATSRoute

@pytest.mark.asyncio
async def test_root():
    # Define a function to send messages
    async def send_messages(route):
        for i in range(10):
            status = await route.send_message(msg=json.dumps({
                "name": f"hello_{i}",
                "hello": "world",
                "world": "goodbye"
            }))
            assert status.status == MessageStatus.QUEUED
            await asyncio.sleep(1)  # 1 second delay between messages
        await route.disconnect()

    # Define a function to listen to messages
    def listen_messages(consumer_provider):
        asyncio.run(consumer_provider.connect())
        while True:
            msg = asyncio.run(consumer_provider.wait())
            print(msg)

    # Create producer and router, and send messages
    provider = NATSMessagingProducerProvider()
    router = Router(provider=provider, yaml_file="./test_routes/test_nats_route.yaml")
    await router.connect()

    route = router.find_router(selector="test/test")
    assert isinstance(route, NATSRoute)
    await asyncio.gather(send_messages(route))

    # Create consumer provider and start listening in a separate thread
    consumer_provider = NATSMessagingConsumerProvider(
        url=route.url,
        name=route.name,
        subject=route.subject
    )

    with ThreadPoolExecutor() as executor:
        executor.submit(listen_messages, consumer_provider)
