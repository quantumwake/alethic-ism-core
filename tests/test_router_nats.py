import asyncio
import json

from ismcore.messaging.base_message_router import Router
from ismcore.messaging.nats_message_provider import NATSMessageProvider


def test_nats_route_1():
    provider = NATSMessageProvider()
    router = Router(
        provider=provider,
        yaml_file="./tests/test_routes/test_nats_route.yaml"
    )

    async def run_test():
        route = router.find_route_by_subject(subject="ism.test")
        await route.connect()
        await route.subscribe()

        async def consumer_loop():
            message = await route.consume(wait=60)
            print(message)

        consumer_task = asyncio.create_task(consumer_loop())

        status = await route.publish(
            msg=json.dumps({
                "name": "hello",
                "hello": "world",
                "world": "goodbye"
            })
        )

        # Optionally, wait for the consumer task to complete if you need the result
        await consumer_task

        return status

    asyncio.get_event_loop().run_until_complete(run_test())



