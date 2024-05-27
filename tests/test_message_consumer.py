import json
from typing import Any

from alethic_ism_core.core.base_message_consumer import BaseMessagingProvider
from alethic_ism_core.core.base_message_consumer_lm import BaseMessagingConsumerLM
from alethic_ism_core.core.base_model import ProcessorProvider, Processor, ProcessorState
from alethic_ism_core.core.base_processor_lm import BaseProcessorLM
from alethic_ism_core.core.processor_state import State
from alethic_ism_core.core.processor_state_storage import StateMachineStorage
from tests.test_metaclass_state_storage import (
    MockStateStorage,
    MockProcessorProviderStorage,
    MockProcessorStorage,
    MockProcessorStateStorage, MockTemplateStorage
)


class MockProcessor(BaseProcessorLM):

    def process_input_data_entry(self, input_query_state: dict, force: bool = False):

        question = input_query_state["question"]
        additional_info_field = input_query_state["additional_info_field"]
        some_other_field = input_query_state["some_other_field"]

        assert question in ["is hello world is the question 1?", "is hello world is the question 2?"]
        assert additional_info_field in ["first other field data 1", "first other field data 2"]
        assert some_other_field in ["second other field data 1", "second other field data 2"]


class MockMessagingProvider(BaseMessagingProvider):

    def __init__(self):
        self.count = 0

    def receive_main(self) -> [Any, Any]:

        # exit the run loop
        if self.count > 0:
            raise InterruptedError()

        self.count += 1

        # mocked provider message object that is acknowledged(...)
        msg = ("mocked consumer message as a string but this is a message object from something "
               "pulsar, kafka, whatever pub/sub system we use")

        processor_id = "20000000-0000-0000-0000-000000000000"
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
            "processor_id": "{processor_id}",
            "query_state": {query_states}
        }}"""

        return msg, data

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
            output_state=output_state
        )

    # def execute(self, message: dict):
    #     super().execute(message=message)


def test_message_consumer():
    test_state_machine_storage = StateMachineStorage(
        state_storage=MockStateStorage(),
        processor_storage=MockProcessorStorage(),
        processor_state_storage=MockProcessorStateStorage(),
        processor_provider_storage=MockProcessorProviderStorage(),
        template_storage=MockTemplateStorage()
    )

    mock_messaging_consumer = MockMessageConsumerProcessor(
        name="mocked messaging consumer lm",
        storage=test_state_machine_storage,
        messaging_provider=MockMessagingProvider()
    )

    mock_messaging_consumer.start_topic_consumer()
    mock_messaging_consumer.RUNNING = False
