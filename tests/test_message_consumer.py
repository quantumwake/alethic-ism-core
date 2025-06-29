import json
from typing import Any, Optional, List
from pydantic import PrivateAttr
from ismcore.messaging.base_message_consumer_processor import BaseMessageConsumerProcessor
from ismcore.messaging.base_message_provider import BaseRouteProvider
from ismcore.messaging.base_message_route_model import BaseRoute, RouteMessageStatus, MessageStatus
from ismcore.messaging.base_message_router import Router
from ismcore.model.base_model import Processor, ProcessorProvider, ProcessorState
from ismcore.model.processor_state import State
from ismcore.processor.base_processor_lm import BaseProcessorLM
from ismcore.storage.processor_state_storage import StateMachineStorage
from tests.test_metaclass_state_storage import (
    MockStateStorage,
    MockProcessorProviderStorage,
    MockProcessorStorage,
    MockProcessorStateRouteStorage, MockTemplateStorage
)


class MockRoute(BaseRoute):

    _count: int = PrivateAttr(default=0)

    def __init__(self, route_config: dict, **kwargs):
        # self.route_config = route_config
        pass

    async def publish(self, msg: str) -> Optional[RouteMessageStatus]:

        msg_json = json.loads(msg)
        if msg_json['status'] == 'FAILED':

            exception = msg_json['exception']
            assert 'no input query state defined' in exception

        return RouteMessageStatus(
            id="test",
            message=msg,
            status=MessageStatus.QUEUED,
            error=None
        )

    # def consume(self) -> [Any, Any]:
    async def consume(self, wait: bool = True):

        # mocked provider message object that is acknowledged(...)
        msg = ("mocked consumer message as a string but this is a message object from something "
               "pulsar, kafka, whatever pub/sub system we use")
        processor_id = "20000000-0000-0000-0000-000000000000"

        if self._count == 0:
            self._count += 1
            query_states = json.dumps([
                {
                    "question": "is hello world is the question 1?",
                    "additional_info_field": "first other field data 1",
                    "some_other_field": "second other field data 1"
                },
                {
                    "question": "is hello world is the question 2?",
                    "additional_info_field": "first other field data 2",
                    "some_other_field": "second other field data 2"
                }
            ])

            # mocked message received on consumer main endpoint
            data = f"""{{
                "type": "query_state",
                "route_id": "10000000-0000-0000-0000-000000000000:{processor_id}",
                "query_state": {query_states}
            }}"""

            return msg, data

        if self._count == 1:
            self._count += 1
            data = f"""{{
                "type": "query_state",
                "route_id": "10000000-0000-0000-0000-000000000000:{processor_id}",
                "query_state": null
            }}"""
            return msg, data

    def close(self):
        pass

    def ack(self, message):
        assert message == ("mocked consumer message as a string but this is a message object from something "
                           "pulsar, kafka, whatever pub/sub system we use")
        # yes we fake acknowledge it


class MockRouteProvider(BaseRouteProvider):
    def create_route(self, route_config: dict) -> BaseRoute:
        return MockRoute(route_config=route_config)


route_provider = MockRouteProvider()
router = Router(
    provider=route_provider,
    yaml_file="./tests/test_routes/test_mock_route.yaml"
)


class MockProcessor(BaseProcessorLM):

    # async def process_input_data(self, input_query_state: dict, force: bool = False):
    async def process_input_data(self, input_data: dict | List[dict], force: bool = False):
        question = input_data["question"]
        additional_info_field = input_data["additional_info_field"]
        some_other_field = input_data["some_other_field"]

        assert question in ["is hello world is the question 1?", "is hello world is the question 2?"]
        assert additional_info_field in ["first other field data 1", "first other field data 2"]
        assert some_other_field in ["second other field data 1", "second other field data 2"]


class MockMessageConsumer(BaseMessageConsumerProcessor):

    def create_processor(self,
                         processor: Processor,
                         provider: ProcessorProvider,
                         output_processor_state: ProcessorState,
                         output_state: State):

        return MockProcessor(
            state_machine_storage=self.storage,
            provider=provider,
            processor=processor,
            output_state=output_state,
            output_processor_state=output_processor_state,
            monitor_route=self.monitor_route
        )


def test_message_pre_post_fail_status():

    mock_monitor_route = router.find_route('test/route/monitor')
    test_state_machine_storage = StateMachineStorage(
        state_storage=MockStateStorage(),
        processor_storage=MockProcessorStorage(),
        processor_state_storage=MockProcessorStateRouteStorage(),
        processor_provider_storage=MockProcessorProviderStorage(),
        template_storage=MockTemplateStorage()
    )

    # find the route to subscribe to
    test_route = router.find_route('test/test')
    mock_messaging_consumer = MockMessageConsumer(
        route=test_route,
        monitor_route=mock_monitor_route,
        storage=test_state_machine_storage)

    # should go through
    mock_messaging_consumer.start_consumer()

    # will throw a value error but check the mock monitor
    mock_messaging_consumer.start_consumer()


def test_message_consumer():
    mock_monitor_route = router.find_route('test/route/monitor')
    test_state_machine_storage = StateMachineStorage(
        state_storage=MockStateStorage(),
        processor_storage=MockProcessorStorage(),
        processor_state_storage=MockProcessorStateRouteStorage(),
        processor_provider_storage=MockProcessorProviderStorage(),
        template_storage=MockTemplateStorage()
    )


    mock_test_consumer_route = "test/test"
    mock_messaging_consumer = MockMessageConsumer(
        route=mock_test_consumer_route,
        storage=test_state_machine_storage,
        monitor_route=mock_monitor_route)

    # with pytest.raises(NotImplementedError) as exc_info:
    mock_messaging_consumer.start_consumer()