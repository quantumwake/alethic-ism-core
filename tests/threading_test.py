import json
import asyncio
import uuid

from alethic_ism_core.core.messaging.base_message_router import Router
from alethic_ism_core.core.messaging.nats_message_provider import NATSMessageProvider

# Create producer and router, and send messages
provider = NATSMessageProvider()
router = Router(provider=provider, yaml_file="./test_routes/test_nats_route.yaml")
route = router.find_route(selector="test/test")

# Create consumer provider and start listening in a separate thread
# consumer_provider = NATSMessagingProvider(
#     url=route.url,
#     name=route.name,
#     subject=route.subject
# )

# run publisher asynchronously, while waiting for the future to complete (testing purposes)
loop = asyncio.get_event_loop()

# loop.run_until_complete(router.connect_all())


async def send_messages():
    # subscribe as a publisher on this route
    random_uuid = str(uuid.uuid4())
    random_uuid = random_uuid[:4]


    for i in range(10):
        status = await route.publish(msg=json.dumps({
            "name": f"hello_{i}{random_uuid}",
            "hello": "world",
            "world": "goodbye"
        }))
        print(f'publishing data {status}')


async def consume_messages():
    # subscribe as a consumer on this route
    await route.subscribe()

    i = 0
    while True:
        msg, data = await route.consume()

        print(f"received data {data}")
        if not msg:
            break

        i += 1
        await route.ack(msg)

    assert i == 10



async def run_all():
    await send_messages()
    await consume_messages()
    await route.disconnect()

loop.run_until_complete(run_all())

