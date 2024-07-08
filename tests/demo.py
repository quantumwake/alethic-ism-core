import asyncio
from alethic_ism_core.core.messaging.nats_message_producer_route import NATSRoute


async def run():
    # provider = NATSMessagingProducerProvider()
    route = NATSRoute(**{
        "name": "route_name",
        "url": "nats://localhost:4222",
        "subject": "test.ism",
        "queue": "test.ism",
        "selector": "test.ism.selector"
    })

    # connect and send test message
    await route.connect()
    await route.send_message("hello world")

asyncio.get_event_loop().run_until_complete(run())