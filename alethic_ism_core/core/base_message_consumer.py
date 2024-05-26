import json
import signal
import sys
import logging as logging
import asyncio

from typing import Any
from pydantic import ValidationError

from .processor_state_storage import StateMachineStorage


class BaseMessagingProvider:

    def close(self):
        raise NotImplementedError()

    def receive_main(self) -> [Any, Any]:
        raise NotImplementedError()

    def receive_management(self) -> [Any, Any]:

        raise NotImplementedError()

    def acknowledge_main(self, message):
        raise NotImplementedError()

    def acknowledge_management(self, message):
        raise NotImplementedError()


class BaseMessagingConsumer:

    def __init__(self, name: str,

                 storage: StateMachineStorage, messaging_provider: BaseMessagingProvider):

        # flag that determines whether to shut down the consumers
        self.RUNNING = False

        # consumer config
        self.name = name
        self.messaging_provider = messaging_provider
        self.storage = storage

    def close(self):
        self.messaging_provider.close()


    async def pre_execute(self, message: dict, **kwargs):
        pass
        # processor.status = StatusCode.RUNNING

    async def fail_execute(self, message: dict, **kwargs):
        pass
        #processor.status = StatusCode.FAILED

    async def post_execute(self, message: dict, **kwargs):
        pass
        # processor.status = StatusCode.COMPLETED

    async def broken(self, exception: Exception, msg: Any):
        logging.error(f"Message validation error: {exception} on data {msg}")
        self.messaging_provider.acknowledge_main(msg)

    async def _execute(self, message: dict):
        try:
            await self.pre_execute(message)
            await self.execute(message)
            await self.post_execute(message)
        except Exception as exception:
            await self.fail_execute(message)
            logging.error(f'critical error {exception}')
        finally:
            pass

    async def execute(self, message: dict):
        raise NotImplementedError()

    async def management_consumer_runloop(self):
        msg = None

        while self.RUNNING:
            msg = None
            try:
                msg, data = self.messaging_provider.receive_management()
                logging.info(f'Message received with {data}')

                # the configuration of the state
                # processor_state = ProcessorState.model_validate_json(data)
                # if processor_state.status in [
                #     ProcessorStatus.TERMINATED,
                #     ProcessorStatus.STOPPED]:
                #     logging.info(f'terminating processor_state: {processor_state}')
                # TODO update the state, ensure that the state information is properly set,
                #  do not forward the msg unless the state has been terminated.

                # else:
                #     logging.info(f'nothing to do for processor_state: {processor_state}')
            except Exception as e:
                logging.error(e)
            finally:
                self.messaging_provider.acknowledge_management(msg)

    async def main_consumer_runloop(self):
        msg = None
        data = None
        self.RUNNING = True

        while self.RUNNING:
            try:
                msg, data = self.messaging_provider.receive_main()
                logging.info(f'Message received with {data}')
                message_dict = json.loads(data)
                await self._execute(message_dict)

                # send ack that the message was consumed.
                self.messaging_provider.acknowledge_main(msg)
            except InterruptedError as e:
                logging.error(f"Stop receiving messages: {e}")
                break
            except ValidationError as e:
                await self.broken(exception=e, msg=msg)
            except Exception as e:
                await self.broken(exception=e, msg=msg)

    def graceful_shutdown(self, signum, frame):
        logging.info("Received SIGTERM signal. Gracefully shutting down.")
        self.RUNNING = False
        sys.exit(0)

    def setup_shutdown_signal(self):
        # Attach the SIGTERM signal handler
        logging.info("setting SIGTERM signal handler")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    def start_topic_consumer(self):
        logging.info('starting up pulsar consumer')
        asyncio.run(self.main_consumer_runloop())


