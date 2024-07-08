import json
import asyncio
from concurrent.futures import ThreadPoolExecutor

from alethic_ism_core.core.messaging.base_message_route_model import MessageStatus
from alethic_ism_core.core.messaging.base_message_router import Router
from alethic_ism_core.core.messaging.nats_message_consumer_provider import NATSMessagingConsumerProvider
from alethic_ism_core.core.messaging.nats_message_producer_provider import NATSMessagingProducerProvider

# Create producer and router, and send messages
provider = NATSMessagingProducerProvider()
router = Router(provider=provider, yaml_file="./test_routes/test_nats_route.yaml")
route = router.find_router(selector="test/test")

# Create consumer provider and start listening in a separate thread
consumer_provider = NATSMessagingConsumerProvider(
    url=route.url,
    name=route.name,
    subject=route.subject
)

# run publisher asynchronously, while waiting for the future to complete (testing purposes)
loop = asyncio.get_event_loop()
loop.run_until_complete(router.connect())


async def send_messages():
    for i in range(10):
        status = await route.send_message(msg=json.dumps({
            "name": f"hello_{i}",
            "hello": "world",
            "world": "goodbye"
        }))
        print(f'publishing data {status}')
        # assert status.status == MessageStatus.QUEUED
        # await asyncio.sleep(0)  # 1 second delay between messages

    # await asyncio.gather(send_messages(route))

# disconnect from the route (wait for completion)


async def listen_messages():
    # Define a function to listen to messages
    await consumer_provider.connect()
    i = 0
    while True:
        msg, data = await consumer_provider.wait()

        if not msg:
            break

        i += 1
        consumer_provider.ack(msg)

    assert i == 10


async def run_all():
    await send_messages()
    await listen_messages()
    await route.disconnect()


loop.run_until_complete(run_all())

