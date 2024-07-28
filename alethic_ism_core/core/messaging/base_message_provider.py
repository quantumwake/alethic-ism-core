import json
import os
import signal
import sys

from .base_message_route_model import BaseRoute
from ..utils.ismlogging import ism_logger

logging = ism_logger(__name__)

FLAG_CONSUMER_WAIT = os.environ.get("FLAG_CONSUMER_WAIT", True)

class BaseRouteProvider:

    def create_route(self, route_config: dict) -> BaseRoute:
        raise NotImplementedError()


class BaseMessageConsumer:

    def __init__(self, route: BaseRoute):
        # flag that determines whether to shut down the consumers
        self.RUNNING = False

        # consumer config
        self.route = route

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
                msg, data = await self.route.consume(wait=FLAG_CONSUMER_WAIT)
                if not msg and not data:        # timed out, returns blank, wait for next iteration
                    continue

                # TODO the ack should happen after the process has been completed
                # await self.route.ack(msg)

                logging.debug(f'Message received with {data}')
                message_dict = json.loads(data)
                status = await self._execute(message_dict)
                logging.debug(f"message id: {self.route.get_message_id(message=msg)}, status: {status}")
            except InterruptedError as e:
                logging.error(f"Stop receiving messages: {e}")
                break
            except Exception as e:
                friendly_messsage = self.route.friendly_message(message=msg)
                logging.warning(f"critical error trying to process message: {friendly_messsage}")
                await self.fail_validate_input_message(consumer_message_mapping=msg, exception=e)
            finally:
                if msg:
                    print(f"********** MESSAGE FINALIZED ON MESSAGE ID: {self.route.get_message_id(msg)}")
                    logging.debug(f"finalizing message id {self.route.get_message_id(msg)}")
                else:
                    logging.warning(f"finalizing message but without a message id, this could be a result of a sudden "
                                    f"broker/consumer termination, between the messaging bus and or ")

                # TODO the ack should be happening at this stage
                await self.route.ack(msg)

    def graceful_shutdown(self, signum, frame):
        logging.info("Received SIGTERM signal. Gracefully shutting down.")
        self.RUNNING = False
        sys.exit(0)

    def setup_shutdown_signal(self):
        # Attach the SIGTERM signal handler
        logging.info("setting SIGTERM signal handler")
        signal.signal(signal.SIGTERM, self.graceful_shutdown)

    async def start_consumer(self, max_loops: int = None, consumer_no: int = 1):
        logging.info(f'starting up consumer {type(self)}')
        await self.route.connect()
        await self.route.subscribe(consumer_no=consumer_no)
        await self.consumer_loop(max_loops=max_loops)
