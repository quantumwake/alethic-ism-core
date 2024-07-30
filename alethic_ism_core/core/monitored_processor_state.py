import json

from typing import Any

from nats.aio.msg import Msg

from .messaging.base_message_route_model import BaseRoute
from .base_model import ProcessorStatusCode
from .utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class MonitoredProcessorState:

    def __init__(self, monitor_route: BaseRoute = None, **kwargs):
        self.monitor_route = monitor_route

    async def send_processor_state_update(
            self,
            route_id: str,
            status: ProcessorStatusCode,
            data: dict = None,
            exception: Any = None):

        monitor_message = json.dumps({
            "type": "processor_state",
            "route_id": route_id,
            "status": status.name,
            "exception": str(exception) if exception else None,
            "data": data
        })

        if not self.monitor_route:
            logging.warning(f'no monitor route defined, unable to provide processor state status updates, '
                            f'here is the monitor message dump instead: {monitor_message}')
            return

        await self.monitor_route.publish(msg=monitor_message)

    async def send_processor_state_from_consumed_message(self,
                                                         consumer_message_mapping: dict,
                                                         status: ProcessorStatusCode,
                                                         exception: Any = None,
                                                         data: dict = None):

        cmm = consumer_message_mapping  # for readability short name
        if isinstance(cmm, Msg):
            try:
                data = cmm.data.decode("utf-8")
                cmm = json.loads(data)
            except:
                raise ValueError(f'unable to extract dictionary from msg {cmm}')

        route_id = cmm['route_id'] if 'route_id' in cmm else None

        await self.send_processor_state_update(
            status=status,
            route_id=route_id,
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
