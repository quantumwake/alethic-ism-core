import logging
import os

from processor.base_processor import initialize_processors_with_same_state_config
from processor.base_question_answer_processor import StateConfigLM
from processor.full_join_processor import FullJoinStateProcessor
from processor.processor_state import StateDataKeyDefinition, State, StateConfig
from processor.question_answer_v2 import AnthropicQuestionAnswerProcessor, OpenAIQuestionAnswerProcessor


LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging.basicConfig(encoding='utf-8', level=LOG_LEVEL)



# initialize a list of processors with the same initial configuration
processors = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction Template 01 (Processor)",
        system_template_path='../templates/animallm/instruction_template_01_system_template.json',
        user_template_path='../templates/animallm/instruction_template_01_user_template.json',
        output_path='../dataset/examples/states',
        output_primary_key_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal")
        ],
        include_extra_from_input_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal")
        ]
    ),
    processor_types=[AnthropicQuestionAnswerProcessor, OpenAIQuestionAnswerProcessor])

# load the initial state objects and merge them
query_template_state = State.load_state('../dataset/animallm/query_template_state.json')
animal_state = State.load_state('../dataset/animallm/animal_state.json')

# the initial state to inject into the next processors (for template 01 question and answering)
animal_query_state_join_processor = FullJoinStateProcessor(
    state=State(
        config=StateConfig(
            name="merged state",
            output_path='../dataset/examples/states',
            output_primary_key_definition=[
                StateDataKeyDefinition(name='animal'),
                StateDataKeyDefinition(name='query')
            ]
            ## TODO add join column key definitions
        )),
    processors=processors
)

# test function
if __name__ == '__main__':
    animal_query_state_join_processor(states=[query_template_state, animal_state])

    pass