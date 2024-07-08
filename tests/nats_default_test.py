import asyncio

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

nc = NATS()

async def do_router():

    route = NATSRoute(

    )

async def do_jetstream():
    subject = "test"

    nc = NATS()
    await nc.connect()

    # Create JetStream context.
    js = nc.jetstream()

    # Persist messages on 'foo's subject.
    await js.add_stream(name="test", subjects=[subject])
    puback = await js.publish(subject=subject, payload=b"hello world")
    print(puback)


async def do_something():
    # Do something with the connection
    await nc.connect(servers=["nats://localhost:4222"])

    nc.publish("test.me", b"hello world")

    await nc.close()


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(do_jetstream())
    # asyncio.run(do_jetstream()

    # asyncio.run(do_something())