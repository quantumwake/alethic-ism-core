import json
import signal
import sys
import asyncio

from typing import Any, Dict, Union

from .base_message_route_model import Route
from .base_model import ProcessorState, ProcessorStatusCode
from .processor_state_storage import StateMachineStorage
from .utils.ismlogging import ism_logger

logging = ism_logger(__name__)

class BaseMessagingProducerProvider:

    def create_route(self, route_config: dict) -> Route:
        raise NotImplementedError()


class BaseMessagingConsumerProvider:

    def __init__(self, main_consumer: Any = None, manage_consumer: Any = None):
        self.main_consumer = main_consumer
        self.manage_consumer = manage_consumer

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

    def get_message_id(self, message):
        raise NotImplementedError()

    def friendly_message(self, message: Any):
        raise NotImplementedError()


class Monitorable:

    def __init__(self, monitor_route: Route = None, **kwargs):
        self.monitor_route = monitor_route

    async def send_processor_state_update(
            self,
            route_id: str,
            status: ProcessorStatusCode,
            data: dict = None,
            exception: Any = None):

        if not self.monitor_route:
            logging.warning(f'no monitor route defined, unable to provide processor state status updates')
            return

        monitor_message = json.dumps({
            "type": "processor_state",
            "route_id": route_id,
            "status": status.name,
            "exception": str(exception) if exception else None,
            "data": data
        })

        self.monitor_route.send_message(msg=monitor_message)

    async def send_processor_state_from_consumed_message(self,
                                                         consumer_message_mapping: dict,
                                                         status: ProcessorStatusCode,
                                                         exception: Any = None,
                                                         data: dict = None):

        cmm = consumer_message_mapping  # for readability short name
        route_id = cmm['route_id'] if 'route_id' in cmm else None

        # user_id = cmm['user_id'] if 'user_id' in cmm else None
        # project_id = cmm['project_id'] if 'project_id' in cmm else None
        # state_id = cmm['input_state_id'] if 'input_state_id' in cmm else None
        # processor_id = cmm['processor_id'] if 'processor_id' in cmm else None

        await self.send_processor_state_update(
            # user_id=user_id,
            # project_id=project_id,
            status=status,
            route_id=route_id,
            # processor_state={
            #     "processor_id": processor_id,
            #     "state_id": state_id,
            #     "direction": direction.name
            # },
            exception=exception,
            data=data
        )

    async def pre_execute(self, consumer_message_mapping: dict, **kwargs):
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.QUEUED
        )

    async def intra_execute(self, consumer_message_mapping: dict, **kwargs):
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.RUNNING
        )

    # TODO probably should not flipflop, we can handle this flipflop in the ism-monitor consumer?
    async def post_execute(self, consumer_message_mapping: dict, **kwargs):
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.COMPLETED
        )

    async def fail_validate_input_message(self, consumer_message_mapping: dict, exception: Any = None):
        # failed to execute validate input message, differs from when a processor fails to execute on a query entry
        await self.send_processor_state_from_consumed_message(
            consumer_message_mapping=consumer_message_mapping,
            status=ProcessorStatusCode.FAILED,
            exception=exception
        )

    async def fail_execute_processor_state(self,
                                           route_id: str,
                                           exception: Any,
                                           data: dict = None,
                                           **kwargs):

        # failed to execute the processor, differs from when a processor fails to execute on a query entry
        await self.send_processor_state_update(
            status=ProcessorStatusCode.FAILED,
            route_id=route_id,
            exception=exception,
            data=data
        )


class BaseMessagingConsumer(Monitorable):

    def __init__(self, name: str,
                 storage: StateMachineStorage,
                 messaging_provider: BaseMessagingConsumerProvider,
                 **kwargs):

        super().__init__(**kwargs)

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

            async def handle_message(msg):
                await self.pre_execute(message)
                await self.execute(message)
                await self.post_execute(message)

            await handle_message(message)
            return True
        except ValueError as e:
            await self.fail_validate_input_message(consumer_message_mapping=message, exception=e)
            return False

    async def execute(self, consumer_message_mapping: dict):
        raise NotImplementedError()

    async def management_consumer_runloop(self):
        raise NotImplementedError('need to implement management topic interface control loop')
        # msg = None
        #
        # while self.RUNNING:
        #     msg = None
        #     try:
        #         msg, data = self.messaging_provider.receive_management()
        #         logging.info(f'Message received with {data}')
        #
        #         # the configuration of the state
        #         # processor_state = ProcessorState.model_validate_json(data)
        #         # if processor_state.status in [
        #         #     ProcessorStatus.TERMINATED,
        #         #     ProcessorStatus.STOPPED]:
        #         #     logging.info(f'terminating processor_state: {processor_state}')
        #         # TODO update the state, ensure that the state information is properly set,
        #         #  do not forward the msg unless the state has been terminated.
        #
        #         # else:
        #         #     logging.info(f'nothing to do for processor_state: {processor_state}')
        #     except Exception as e:
        #         logging.error(e)
        #     finally:
        #         self.messaging_provider.acknowledge_management(msg)

    async def main_consumer_runloop(self, max_loops: int = None):
        msg = None
        data = None
        self.RUNNING = True

        loop_count = 0
        while self.RUNNING:

            # if the maximum loop is defined and the threshold has reached
            if max_loops and loop_count >= max_loops:
                logging.info(f'stopping receiver from loop {loop_count} of max loops: {max_loops}')
                break

            msg = None
            loop_count += 1
            try:
                msg, data = self.messaging_provider.receive_main()
                print(f"********** MESSAGE RECEIVED ON MESSAGE ID: {self.messaging_provider.get_message_id(msg)}")
                logging.info(f'Message received with {data}')
                message_dict = json.loads(data)
                status = await self._execute(message_dict)
                logging.debug(f"message id: {self.messaging_provider.get_message_id(message=msg)}, status: {status}")

                # send ack that the message was consumed.
                # self.messaging_provider.acknowledge_main(msg)
            except InterruptedError as e:
                logging.error(f"Stop receiving messages: {e}")
                break
            except Exception as e:
                friendly_messsage = self.messaging_provider.friendly_message(message=msg)
                logging.warning(f"critical error trying to process message: {friendly_messsage}")
                await self.fail_validate_input_message(consumer_message_mapping=msg, exception=e)
            finally:
                if msg:
                    print(f"********** MESSAGE FINALIZED ON MESSAGE ID: {self.messaging_provider.get_message_id(msg)}")
                    logging.debug(f"finalizing message id {self.messaging_provider.get_message_id(msg)}")
                else:
                    logging.warning(f"finalizing message but without a message id, this could be a result of a sudden "
                                    f"broker/consumer termination, between the messaging bus and or ")

                self.messaging_provider.acknowledge_main(msg)
                pass

    def graceful_shutdown(self, signum, frame):
        logging.info("Received SIGTERM signal. Gracefully shutting down.")
        self.RUNNING = False
        sys.exit(0)

    def setup_shutdown_signal(self):
        # Attach the SIGTERM signal handler
        logging.info("setting SIGTERM signal handler")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    def start_topic_consumer(self, max_loops: int = None):
        logging.info('starting up pulsar consumer')
        asyncio.run(self.main_consumer_runloop(max_loops=max_loops))
