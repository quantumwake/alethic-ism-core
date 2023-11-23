import logging as log
import os

from processor.base_processor import initialize_processors_with_same_state_config
from processor.base_question_answer_processor import StateConfigLM
from processor.processor_full_join import FullJoinStateProcessor
from processor.processor_state import StateDataKeyDefinition, State, StateConfig, print_state_information
from processor.processor_question_answer import AnthropicQuestionAnswerProcessor, OpenAIQuestionAnswerProcessor


LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging = log.getLogger(__name__)
log.basicConfig(encoding='utf-8', level=LOG_LEVEL)

# initialize a list of processors with the same initial configuration
processors_query_response_animal_perspective = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction for Query Animal Perspective Response",
        # system_template_path='../templates/animallm/instruct.json',
        user_template_path='../templates/animallm/instruction_template_01_query_response_animal.json',
        output_path='../states',
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

# initialize a list of processors with the same initial configuration
processors_query_response_other_perspective = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction for Query Animal Perspective Response",
        # system_template_path='../templates/animallm/instruct.json',
        user_template_path='../templates/animallm/instruction_template_02_query_response_perspective.json',
        output_path='../states',
        output_primary_key_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal"),
            StateDataKeyDefinition(name='perspective')
        ],
        include_extra_from_input_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal")
        ]
    ),
    processor_types=[OpenAIQuestionAnswerProcessor])

#AnthropicQuestionAnswerProcessor,


# load the initial state objects and merge them
query_template_state = State.load_state('../dataset/animallm/query_template_state_devtest.json')
animal_state = State.load_state('../dataset/animallm/animal_state_devtest.json')

# the initial state to inject into the next processors (for template 01 question and answering)
animal_query_state_join_processor = FullJoinStateProcessor(
    state=State(
        config=StateConfig(
            name="merged state",
            output_path='../states/animallm/devtest',
            output_primary_key_definition=[
                StateDataKeyDefinition(name='animal'),
                StateDataKeyDefinition(name='query')
            ],
            template_columns=[
                StateDataKeyDefinition(name='query')
            ],
            remap_query_state_columns=[
                StateDataKeyDefinition(name='query_template', alias='query')
            ]
            ## TODO add join column key definitions or create a new processor type for joinable by keys
        )),
    processors=processors_query_response_animal_perspective
)

# test function
if __name__ == '__main__':
    # animal_query_state_join_processor(states=[query_template_state, animal_state])
    #
    # print(animal_query_state_join_processor.build_final_output_path())
    # print_state_information('../states/animallm/devtest/')
    # processor_1 = animal_query_state_join_processor.processors[0]

    state_merger = FullJoinStateProcessor(
        state=State(
            config=StateConfig(
                name="merged state 2",
                output_path='../states/animallm/devtest',
                output_primary_key_definition=[
                    StateDataKeyDefinition(name='animal'),
                    StateDataKeyDefinition(name='query')
                ],
                template_columns=[
                    StateDataKeyDefinition(name='query')
                ],
                remap_query_state_columns=[
                    StateDataKeyDefinition(name='query_template', alias='query')
                ]
                ## TODO add join column key definitions
            )),
        processors=processors_query_response_other_perspective
    )

    # test_proc = processors[1]

    query_template_state = State.load_state('../dataset/animallm/query_template_state_devtest.json')
    animal_state = State.load_state('../dataset/animallm/animal_state_devtest.json')
    perspective_definitions = State.load_state('../dataset/animallm/perspective_definition_devtest.json')

    state_merger(states=[animal_state, query_template_state, perspective_definitions])


    pass