import asyncio
from typing import List

import pytest

from ismcore.model.base_model import InstructionTemplate, ProcessorProvider, Processor, ProcessorState, \
    ProcessorStateDirection
from ismcore.model.processor_state import State, StateConfigLM, StateDataKeyDefinition, StateDataColumnDefinition, \
    StateConfig
from ismcore.processor.base_processor import BaseProcessor
from ismcore.processor.base_processor_lm import BaseProcessorLM
from ismcore.storage.processor_state_storage import StateMachineStorage

input_query_states_1 = [
    {"question": "what color is the sky?"},
    {"question": "what color is the grass?"},
]

input_query_states_2 = [
    {'name': 'Alice', 'age': 30, 'job': 'Developer'},
    {'name': 'Bob',   'age': 25, 'job': 'Designer'},
    {'name': 'Carol', 'age': 28, 'job': 'Data Scientist'},
]

class MockStateMachineStorage(StateMachineStorage):

    def fetch_template(self, template_id: str) -> InstructionTemplate | None:
        if template_id == "test_template_id_1":
            return InstructionTemplate(
                template_id="test_template_id_1",
                template_path="test_template_path_1",
                template_content="answer the following question: ${question}",
                template_type="mako",
                project_id="test_project_id_1"
            )
        elif template_id == "test_template_id_2":
            tmpl = """
               Items:
               % for it in items:
                 - Name: ${it['name']}, Age: ${it['age']}, Job: ${it['job']}
               % endfor
               """
            return InstructionTemplate(
                template_id="test_template_id_2",
                template_path="test_template_path_2",
                template_content=tmpl,
                template_type="mako",
                project_id="test_project_id_1"
            )

    def fetch_processor(self, processor_id: str) -> Processor:
        return Processor(
            id="test processor id",
            provider_id="test provider id",
            project_id="test project id"
        )


class MockProcessorLM(BaseProcessorLM):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    async def _execute(self, user_prompt: str, system_prompt: str, values: dict | List[dict]):
        response = None
        if isinstance(values, dict):
            question = values["question"]
            if question == 'what color is the sky?':
                response = {"response": "the sky is blue"}

            if question == 'what color is the grass?':
                response = {"response": "the grass is green"}
        else:
            response = {"response": "the beach is sandy"}

        return response, dict, str(response)


def failure_callback_handler(processor_state: ProcessorState, exception: Exception, query_state: dict):
    pass


def test_mock_processor_lm_entry() :
    mock_processor = setup_mock_processor_lm(user_template_id="test_template_id_1")
    mock_processor.config.flag_enable_execute_set = False
    response_1 = asyncio.run(mock_processor.process_input_data(input_data=input_query_states_1[0]))
    response_2 = asyncio.run(mock_processor.process_input_data(input_data=input_query_states_1[1]))

    assert response_1[0]['response'] == 'the sky is blue'
    assert response_2[0]['response'] == 'the grass is green'
    assert response_2[0]['provider_name'] == 'test provider name'
    assert response_2[0]['provider_version'] == 'test-version-1.0'

def test_mock_processor_lm_entry_set() :
    mock_processor = setup_mock_processor_lm(user_template_id="test_template_id_2")
    mock_processor.config.flag_enable_execute_set = True
    response_1 = asyncio.run(mock_processor.process_input_data(input_data=input_query_states_2))

    assert response_1[0]['response'] == 'the beach is sandy'


def setup_mock_processor_lm(user_template_id: str) -> MockProcessorLM:
    storage = MockStateMachineStorage()
    output_state = State(
        id="test output state mock",
        config=StateConfigLM(
            name="Test Output State",
            storage_class="memory",
            user_template_id=user_template_id,
            primary_key=[
                StateDataKeyDefinition(name="question")
            ]
        )
    )
    output_state.columns = {
        "provider_name": StateDataColumnDefinition(name="provider_name", value="provider.name", callable=True),
        "provider_version": StateDataColumnDefinition(name="provider_version", value="provider.version", callable=True),
    }

    # configure the provider, the processor "type" for the provider, and the output edges where the data should flow next
    provider = ProcessorProvider(id="test provider id", name="test provider name", version="test-version-1.0", class_name="MockProviders")
    processor = Processor(id="test processor id", provider_id=provider.id, project_id="test project id")
    output_processor_state = ProcessorState(id=f"{processor.id}:{output_state.id}", state_id=output_state.id, processor_id=processor.id, direction=ProcessorStateDirection.OUTPUT)

    # set up the actual mock processor that executes the code
    return MockProcessorLM(
        monitor_route=None, # set the monitor route none, not testing this
        provider=provider,
        processor=processor,
        output_processor_state=output_processor_state,
        output_state=output_state,
        state_machine_storage=storage,
        failure_callback=failure_callback_handler
    )


#
# @staticmethod
# @pytest.mark.asyncio
# async def test_mock_processor():
#     storage = MockStateMachineStorage()
#
#     output_state = State(
#         config=StateConfig(
#             name="Test Output State",
#             storage_class="memory",
#             primary_key=[
#                 StateDataKeyDefinition(name="question")
#             ]
#         )
#     )
#
#     provider = ProcessorProvider(id="test provider id", name="test provider name", version="test-version-1.0", class_name="MockProviders")
#     processor = Processor(id="test processor id", provider_id=provider.id, project_id="test project id")
#     processor_executor = MockProcessor1(provider=provider, processor=processor, state_machine_storage=storage, output_state=output_state)
#
#     input_query_states = [
#         {"question": "what color is the sky?"},
#         {"question": "what color is the grass?"},
#     ]
#
#     response_1 = await processor_executor.execute(input_query_state=input_query_states[0])
#     response_2 = await processor_executor.execute(input_query_state=input_query_states[1])
#
#     assert response_1['response'] == 'the sky is blue'
#     assert response_2['response'] == 'the grass is green'
