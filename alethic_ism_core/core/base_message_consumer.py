import json
import signal
import sys
from typing import Any

import pulsar
import asyncio
from pydantic import ValidationError
import logging as logging
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

    def __init__(self, name: str,  storage: StateMachineStorage, messaging_provider: BaseMessagingProvider):

        # flag that determines whether to shut down the consumers
        self.RUNNING = False

        # consumer config
        self.name = name
        self.messaging_provider = messaging_provider
        self.storage = storage

    def close(self):
        self.messaging_provider.close()

    async def _execute(self, message: dict):
        try:
            await self.execute(message)
            pass
        except Exception as exception:
            # processor_state.status = ProcessorStatus.FAILED
            logging.error(f'critical error {exception}')
        finally:
            pass
            # state_storage.update_processor_state(processor_state=processor_state)

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


                # if 'state_id' not in message_dict:

                # TODO check whether the message is for the appropriate processor

                # the configuration of the state
                # processor_state = ProcessorState.model_validate_json(data)
                # processor_state = state_storage.fetch_processor_states_by(
                #     processor_id=processor_state.processor_id,
                #     input_state_id=processor_state.input_state_id,
                #     output_state_id=processor_state.output_state_id
                # )
                # if processor_state.status in [ProcessorStatus.QUEUED, ProcessorStatus.RUNNING]:
                #     await execute(processor_state=processor_state)
                # else:
                #     logging.error(f'status not in QUEUED, unable to processor state: {processor_state}  ')

                # send ack that the message was consumed.
                self.messaging_provider.acknowledge_main(msg)

                # Log success
                # logger.info(
                #     f"Message successfully consumed and stored with asset id {asset.id} for account {asset.library_id}")
            except InterruptedError:
                logging.error("Stop receiving messages")
                break
            except ValidationError as e:
                # it is safe to assume that if we get a validation error, there is a problem with the json object
                # TODO throw into an exception log or trace it such that we can see it on a dashboard
                self.messaging_provider.acknowledge_main(msg)
                logging.error(f"Message validation error: {e} on data {data}")
            except Exception as e:
                self.messaging_provider.acknowledge_main(msg)
                # TODO need to send this to a dashboard, all excdptions in consumers need to be sent to a dashboard
                logging.error(f"An error occurred: {e} on data {data}")

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


