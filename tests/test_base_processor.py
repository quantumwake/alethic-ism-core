import asyncio

import pytest

from alethic_ism_core.core.base_model import (
    InstructionTemplate,
    ProcessorProvider,
    ProcessorState,
    Processor,
    ProcessorStateDirection
)

from alethic_ism_core.core.processor_state import (
    State,
    StateConfig,
    StateDataKeyDefinition,
    StateConfigLM,
    StateDataColumnDefinition
)

from alethic_ism_core.core.base_processor import BaseProcessor
from alethic_ism_core.core.base_processor_lm import BaseProcessorLM
from alethic_ism_core.core.processor_state_storage import StateMachineStorage

input_query_states = [
    {"question": "what color is the sky?"},
    {"question": "what color is the grass?"},
]


def mock_question_response(input_query_state: dict):
    question = input_query_state['question']

    if question == 'what color is the sky?':
        return {
            **input_query_state,
            **{"response": "the sky is blue"}
        }
    if question == 'what color is the grass?':
        return {
            **input_query_state,
            **{"response": "the grass is green"}
        }

    raise NotImplemented(f'invalid question {question}')


class MockStateMachineStorage(StateMachineStorage):

    def fetch_template(self, template_id: str) -> InstructionTemplate:
        return InstructionTemplate(
            template_id="test_template_id_1",
            template_path="test_template_path_1",
            template_content="answer the following question: {question}",
            template_type="user template",
            project_id="test_project_id_1"
        )


class MockProcessor(BaseProcessor):

    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        return mock_question_response(input_query_state=input_query_state)


class MockProcessorLM(BaseProcessorLM):

    def __init__(self, output_state: dict, **kwargs):

        super().__init__(
            monitor_route=None,
            output_state=output_state,
            **kwargs)

        self.provider = ProcessorProvider(
            id="test provider id",
            name="test provider name",
            version="test-version-1.0",
            class_name="MockProviders"
        )

        self.processor = Processor(
            id="test processor id",
            provider_id=self.provider.id,
            project_id="test project id"
        )

        self.output_processor_state = ProcessorState(
            id=f"{self.processor.id}:{self.output_state.id}",
            state_id=self.output_state.id,
            processor_id=self.processor.id,
            direction=ProcessorStateDirection.OUTPUT
        )

    async def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        question = values['question']
        response_dict = mock_question_response(input_query_state=values)
        return response_dict, dict, str(response_dict)

    def __process_input_data_entry(self, input_query_state: dict, force: bool = False):
        if input_query_state['question'] == 'what color is the sky?':
            return {
                **input_query_state,
                **{"response": "the sky is blue"}
            }
        if input_query_state['question'] == 'what color is the grass?':
            return {
                **input_query_state,
                **{"response": "the grass is green"}
            }


def test_mock_processor_lm():
    storage = MockStateMachineStorage()
    output_state = State(
        id="test output state mock",
        config=StateConfigLM(
            name="Test Output State",
            storage_class="memory",
            user_template_id="test_template_id_1",
            primary_key=[
                StateDataKeyDefinition(name="question")
            ]
        )
    )
    output_state.columns = {
        "provider_name": StateDataColumnDefinition(name="provider_name", value="provider.name", callable=True),
        "provider_version": StateDataColumnDefinition(name="provider_version", value="provider.version", callable=True),
    }

    def failure_callback_handler(processor_state: ProcessorState, exception: Exception, query_state: dict):
        pass

    mock_processor = MockProcessorLM(
        output_state=output_state,
        state_machine_storage=storage,
        failure_callback=failure_callback_handler
    )

    response_1 = asyncio.run(mock_processor.process_input_data_entry(input_query_state=input_query_states[0]))
    response_2 = asyncio.run(mock_processor.process_input_data_entry(input_query_state=input_query_states[1]))

    assert response_1[0]['response'] == 'the sky is blue'
    assert response_2[0]['response'] == 'the grass is green'
    assert response_2[0]['provider_name'] == 'test provider name'
    assert response_2[0]['provider_version'] == 'test-version-1.0'


@staticmethod
@pytest.mark.asynico
async def test_mock_processor():
    storage = MockStateMachineStorage()

    output_state = State(
        config=StateConfig(
            name="Test Output State",
            storage_class="memory",
            primary_key=[
                StateDataKeyDefinition(name="question")
            ]
        )
    )

    provider = ProcessorProvider(
        id="test provider id",
        name="test provider name",
        version="test-version-1.0",
        class_name="MockProviders"
    )

    mock_processor = MockProcessor(provider=provider, state_machine_storage=storage, output_state=output_state)

    input_query_states = [
        {"question": "what color is the sky?"},
        {"question": "what color is the grass?"},
    ]

    response_1 = mock_processor.execute(input_query_state=input_query_states[0])
    response_2 = mock_processor.execute(input_query_state=input_query_states[1])

    assert response_1['response'] == 'the sky is blue'
    assert response_2['response'] == 'the grass is green'
