import asyncio
import time
from typing import Any

from alethic_ism_core.core.messaging.base_message_route_model import BaseRoute
from alethic_ism_core.core.messaging.nats_message_route import NATSRoute

async def run_consumer():
    async def callback(route: BaseRoute, msg: Any, data: Any):
        print(msg, data)
        await route.ack(msg)

    nat_route = NATSRoute(
        selector="test/route",
        name="test_route",
        subject="test.route",
        url="nats://localhost:4222",
        jetstream=True,
        callback=callback,
    )

    connected = await nat_route.connect()
    assert connected
    await nat_route.subscribe()

    async def stop_consumer():
        await asyncio.sleep(5)  # Use asyncio.sleep instead of time.sleep
        # await nat_route.disconnect()
        nat_route.active = False

    asyncio.create_task(stop_consumer())

    await nat_route.consume(wait=True)


async def run_publisher():
    nat_route = NATSRoute(
        selector="test/route",
        name="test_route",
        subject="test.route",
        url="nats://localhost:4222",
        jetstream=True,
    )

    connected = await nat_route.connect()
    assert connected

    for index in range(10):
        print(f"Publishing message {index}")
        await nat_route.publish(f"Hello World {index}")
        await nat_route.flush()
        await asyncio.sleep(0.1)  # Add a small delay if necessary


async def main():
    publisher_task = asyncio.create_task(run_publisher())
    consumer_task = asyncio.create_task(run_consumer())
    await asyncio.gather(publisher_task, consumer_task)

if __name__ == '__main__':
    asyncio.run(main())