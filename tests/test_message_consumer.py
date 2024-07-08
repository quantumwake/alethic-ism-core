import json
from typing import Any

from alethic_ism_core.core.messaging.base_message_consumer_lm import BaseMessagingConsumerLM
from alethic_ism_core.core.messaging.base_message_provider import BaseMessagingConsumerProvider, BaseMessagingProducerProvider
from alethic_ism_core.core.messaging.base_message_route_model import Route, RouteMessageStatus, MessageStatus
from alethic_ism_core.core.messaging.base_message_router import Router
from alethic_ism_core.core.base_model import ProcessorProvider, Processor, ProcessorState
from alethic_ism_core.core.base_processor_lm import BaseProcessorLM
from alethic_ism_core.core.processor_state import State
from alethic_ism_core.core.processor_state_storage import StateMachineStorage
from tests.test_metaclass_state_storage import (
    MockStateStorage,
    MockProcessorProviderStorage,
    MockProcessorStorage,
    MockProcessorStateRouteStorage, MockTemplateStorage
)


class MockRoute(Route):

    def __init__(self, **data):
        super().__init__(**data)

        rc = data['route_config']

    def send_message(self, msg: Any) -> RouteMessageStatus:

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

class MockMessagingProducerProvider(BaseMessagingProducerProvider):
    def create_route(self, route_config: dict) -> Route:
        return MockRoute(route_config=route_config)


messaging_provider = MockMessagingProducerProvider()
router = Router(
    provider=messaging_provider,
    yaml_file="./test_routes/test_mock_route.yaml"
)


class MockProcessor(BaseProcessorLM):

    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        question = input_query_state["question"]
        additional_info_field = input_query_state["additional_info_field"]
        some_other_field = input_query_state["some_other_field"]

        assert question in ["is hello world is the question 1?", "is hello world is the question 2?"]
        assert additional_info_field in ["first other field data 1", "first other field data 2"]
        assert some_other_field in ["second other field data 1", "second other field data 2"]


class MockMessagingConsumerProvider(BaseMessagingConsumerProvider):

    def __init__(self):
        self.count = 0

    def receive_main(self) -> [Any, Any]:

        # mocked provider message object that is acknowledged(...)
        msg = ("mocked consumer message as a string but this is a message object from something "
               "pulsar, kafka, whatever pub/sub system we use")
        processor_id = "20000000-0000-0000-0000-000000000000"

        if self.count == 0:
            self.count += 1
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

        if self.count == 1:
            self.count += 1
            data = f"""{{
                "type": "query_state",
                "route_id": "10000000-0000-0000-0000-000000000000:{processor_id}",
                "query_state": null
            }}"""
            return msg, data

        # if self.count == 2:
        #     self.count += 1
        #     raise Exception(f'fake exception to see handling of error handling')
        #
        # # exit the run loop
        # if self.count == 3:
        #     self.count += 1
        #     raise InterruptedError()

    def receive_management(self) -> [Any, Any]:
        raise InterruptedError()

    def close(self):
        pass

    def acknowledge_main(self, message):
        assert message == ("mocked consumer message as a string but this is a message object from something "
                           "pulsar, kafka, whatever pub/sub system we use")
        # yes we fake acknowledge it

    def acknowledge_management(self, message):
        # yes we fake acknowledge it
        pass


class MockMessageConsumerProcessor(BaseMessagingConsumerLM):

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

    mock_monitor_route = router.find_router('test/route/monitor')
    test_state_machine_storage = StateMachineStorage(
        state_storage=MockStateStorage(),
        processor_storage=MockProcessorStorage(),
        processor_state_storage=MockProcessorStateRouteStorage(),
        processor_provider_storage=MockProcessorProviderStorage(),
        template_storage=MockTemplateStorage()
    )

    mock_messaging_consumer = MockMessageConsumerProcessor(
        name="mocked messaging consumer lm",
        storage=test_state_machine_storage,
        messaging_provider=MockMessagingConsumerProvider(),
        monitor_route=mock_monitor_route
    )

    # should go through
    mock_messaging_consumer.start_topic_consumer(max_loops=1)

    # will throw a value error but check the mock monitor
    mock_messaging_consumer.start_topic_consumer(max_loops=1)


def test_message_consumer():
    mock_monitor_route = router.find_router('test/route/monitor')
    test_state_machine_storage = StateMachineStorage(
        state_storage=MockStateStorage(),
        processor_storage=MockProcessorStorage(),
        processor_state_storage=MockProcessorStateRouteStorage(),
        processor_provider_storage=MockProcessorProviderStorage(),
        template_storage=MockTemplateStorage()
    )

    mock_messaging_consumer = MockMessageConsumerProcessor(
        name="mocked messaging consumer lm",
        storage=test_state_machine_storage,
        messaging_provider=MockMessagingConsumerProvider(),
        monitor_route=mock_monitor_route
    )

    # with pytest.raises(NotImplementedError) as exc_info:
    mock_messaging_consumer.start_topic_consumer(1)