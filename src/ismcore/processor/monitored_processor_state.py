import datetime as dt
import json

from typing import Any
from nats.aio.msg import Msg

from ismcore.messaging.base_message_route_model import BaseRoute
from ismcore.model.base_model import Usage, UnitType, UnitSubType, ProcessorStatusCode
from ismcore.utils.ism_logger import ism_logger

logging = ism_logger(__name__)


class MonitoredUsage:
    def __init__(self, usage_route: BaseRoute = None, **kwargs):
        self.usage_route = usage_route

    async def publish_usage(self, usage: Usage):
        if not self.usage_route:
            logging.error(f"no usage route set, cannot send processor usage details to downstream usage consumer")
            return

        json_dump = usage.model_dump_json()
        result = await self.usage_route.publish(json_dump)
        return result

    async def send_usage_input_tokens(self, count: int):
        # track input token count
        usage = Usage(
            resource_id=self.processor.id, resource_type=self.provider.id,
            transaction_time=dt.datetime.utcnow(), project_id=self.processor.project_id,
            unit_type=UnitType.TOKEN, unit_subtype=UnitSubType.INPUT, unit_count=count,
        )
        await self.publish_usage(usage)

    async def send_usage_output_tokens(self, count: int):
        # track output token count
        usage = Usage(
            resource_id=self.processor.id, resource_type=self.provider.id,
            transaction_time=dt.datetime.utcnow(), project_id=self.processor.project_id,
            unit_type=UnitType.TOKEN, unit_subtype=UnitSubType.OUTPUT, unit_count=count,
        )
        await self.publish_usage(usage)


class MonitoredProcessorState:

    def __init__(self, monitor_route: BaseRoute | None = None, **kwargs):
        super().__init__(**kwargs)
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
