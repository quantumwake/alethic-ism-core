import json
import os
import signal
import sys
from typing import Any

from .base_message_route_model import BaseRoute
from ..monitored_processor_state import MonitoredProcessorState
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)

FLAG_CONSUMER_WAIT = os.environ.get("FLAG_CONSUMER_WAIT", True)


class BaseRouteProvider:

    def create_route(self, route_config: dict) -> BaseRoute:
        raise NotImplementedError()


class BaseMessageConsumer(MonitoredProcessorState):

    def __init__(self, route: BaseRoute, monitor_route: BaseRoute):
        # flag that determines whether to shut down the consumers
        self.RUNNING = False

        # consumer config
        self.route = route
        self.monitor_route = monitor_route

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

    async def on_receive(self, msg: Any, data: Any):
        try:
            id = self.route.get_message_id(msg)
            logging.debug(f'received with message id: {id}')
            message_dict = json.loads(data)
            status = await self._execute(message_dict)
            logging.debug(f"message id: {id}, status: {status}")
        except Exception as e:
            friendly_msg = self.route.friendly_message(message=msg)
            logging.warning(f"critical error trying to process message: {friendly_msg} error: {e}")
            await self.fail_validate_input_message(consumer_message_mapping=msg, exception=e)
        finally:
            acked = await self.route.ack(msg)
            logging.debug(f"finalizing message id {self.route.get_message_id(msg)}, acked: {acked}")

    async def consumer_loop(self, max_loops: int = None):
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
                await self.route.consume(wait=FLAG_CONSUMER_WAIT)
                if not msg:        # timed out, returns blank, wait for next iteration
                    continue
            except InterruptedError as e:
                logging.error(f"Stop receiving messages: {e}")
                break

    def graceful_shutdown(self, signum, frame):
        logging.info("Received SIGTERM signal. Gracefully shutting down.")
        self.RUNNING = False
        sys.exit(0)

    def setup_shutdown_signal(self):
        # Attach the SIGTERM signal handler
        logging.info("setting SIGTERM signal handler")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    async def start_consumer(self):
        logging.info(f'starting up consumer {type(self)}')
        self.route.callback = self.on_receive
        await self.route.connect()
        await self.route.subscribe()
        # await self.consumer_loop()
