from alethic_ism_core.core.base_model import InstructionTemplate
from alethic_ism_core.core.base_processor import BaseProcessor
from alethic_ism_core.core.base_processor_lm import BaseProcessorLM
from alethic_ism_core.core.processor_state import State, StateConfig, StateDataKeyDefinition, StateConfigLM
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
            template_id = "test_template_id_1",
            template_path="test_template_path_1",
            template_content="answer the following question: {question}",
            template_type="user template",
            project_id="test_project_id_1"
        )

class MockProcessor(BaseProcessor):

    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        return mock_question_response(input_query_state=input_query_state)


class MockProcessorLM(BaseProcessorLM):

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
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
        config=StateConfigLM(
            name="Test Output State",
            storage_class="memory",
            user_template_id="test_template_id_1",
            primary_key=[
                StateDataKeyDefinition(name="question")
            ]
        )
    )

    mock_processor = MockProcessorLM(state_machine_storage=storage, output_state=output_state)
    response_1 = mock_processor.process_input_data_entry(input_query_state=input_query_states[0])
    response_2 = mock_processor.process_input_data_entry(input_query_state=input_query_states[1])

    assert response_1['response'] == 'the sky is blue'
    assert response_2['response'] == 'the grass is green'


def test_mock_processor():

    output_state = State(
        config=StateConfig(
            name="Test Output State",
            storage_class="memory",
            primary_key=[
                StateDataKeyDefinition(name="question")
            ]
        )
    )
    mock_processor = MockProcessor(output_state=output_state)

    input_query_states = [
        {"question": "what color is the sky?"},
        {"question": "what color is the grass?"},
    ]

    response_1 = mock_processor.process_input_data_entry(input_query_state=input_query_states[0])
    response_2 = mock_processor.process_input_data_entry(input_query_state=input_query_states[1])

    assert response_1['response'] == 'the sky is blue'
    assert response_2['response'] == 'the grass is green'
