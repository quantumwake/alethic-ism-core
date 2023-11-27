import logging as log
import os

from processor.base_processor import initialize_processors_with_same_state_config
from processor.base_question_answer_processor import StateConfigLM
from processor.processor_full_join import FullJoinStateProcessor
from processor.processor_state import StateDataKeyDefinition, State, StateConfig
from processor.processor_question_answer import AnthropicQuestionAnswerProcessor, OpenAIQuestionAnswerProcessor


LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging = log.getLogger(__name__)
log.basicConfig(encoding='utf-8', level=LOG_LEVEL)

# initialize a list of processors with the same initial configuration
instruction_template_P0_query_response_animal = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction for P0 response",
        version="Draft Version 0.1",
        # system_template_path='../templates/animallm/instruct.json',
        user_template_path='../templates/animallm/instruction_template_P0_query_response_animal.json',
        output_path='../states/animallm/prod',
        output_primary_key_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal")
        ],
        include_extra_from_input_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal"),
            StateDataKeyDefinition(name="query_template_id", alias="query_template_id")
        ]
    ),
    processor_types=[OpenAIQuestionAnswerProcessor, AnthropicQuestionAnswerProcessor])

#### *# AnthropicQuestionAnswerProcessor

instruction_template_P1_query_response_default = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction for P1 response",
        version="Draft Version 0.1",
        # system_template_path='../templates/animallm/instruct.json',
        user_template_path='../templates/animallm/instruction_template_P1_query_response_default_perspective.json',
        output_path='../states/animallm/prod',
        output_primary_key_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal")
        ],
        include_extra_from_input_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal"),
            StateDataKeyDefinition(name="query_template_id", alias="query_template_id")
        ]
    ),
    processor_types=[OpenAIQuestionAnswerProcessor, AnthropicQuestionAnswerProcessor])


# initialize a list of processors with the same initial configuration
instruction_template_Pn_query_response_perspectives = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction for Query Response Perspective P(n)",
        version="Draft Version 0.1",
        # system_template_path='../templates/animallm/instruct.json',
        user_template_path='../templates/animallm/instruction_template_P(n)_query_response_perspective.json',
        output_path='../states/animallm/prod',
        output_primary_key_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal"),
            StateDataKeyDefinition(name='perspective')
        ],
        include_extra_from_input_definition=[
            StateDataKeyDefinition(name="query", alias="query"),
            StateDataKeyDefinition(name="animal", alias="animal"),
            StateDataKeyDefinition(name='perspective'),
            StateDataKeyDefinition(name='perspective_index'),
            StateDataKeyDefinition(name="query_template_id", alias="query_template_id")
        ]
    ),
    processor_types=[OpenAIQuestionAnswerProcessor, AnthropicQuestionAnswerProcessor])


processors_query_response_p0_evaluator_openai = OpenAIQuestionAnswerProcessor(
    state=State(
        config=StateConfigLM(
            version="Draft Version 0.1",
            name="AnimaLLM Instruction for Query Response Evaluation P0",
            user_template_path='../templates/animallm/instruction_template_P0_evaluator.json',
            output_path='../states/animallm/prod',
            output_primary_key_definition=[
                StateDataKeyDefinition(name="query", alias="query"),
                StateDataKeyDefinition(name="animal", alias="animal"),
                StateDataKeyDefinition(name="perspective_index", alias="perspective_index"),
                StateDataKeyDefinition(name='response_provider_name'),
                StateDataKeyDefinition(name='response_model_name')
            ],
            include_extra_from_input_definition=[
                StateDataKeyDefinition(name="query", alias="query"),
                StateDataKeyDefinition(name="animal", alias="animal"),
                StateDataKeyDefinition(name='perspective'),
                StateDataKeyDefinition(name='perspective_index'),
                StateDataKeyDefinition(name='response_provider_name'),
                StateDataKeyDefinition(name='response_model_name'),
                StateDataKeyDefinition(name='response'),
                StateDataKeyDefinition(name='justification'),
                StateDataKeyDefinition(name="query_template_id", alias="query_template_id")
            ]
        )
    )
)

## model, query, perspective, perspective index, response, score_type, score


## Model{InputTemplate#→PerspectiveN[OutputResponse]}
# ⇛ generates S1[Model{InputTemplate#→PerspectiveN[OutputResponse]}]
#   and S2[Model{InputTemplate#→PerspectiveN[OutputResponse]}].

# load the initial state objects and merge them
perspective_definitions = State.load_state('../dataset/animallm/perspective_definition.json')
query_template_state = State.load_state('../dataset/animallm/query_template_state.json')
animal_state = State.load_state('../dataset/animallm/animal_state.json')

net_new = []
net_new.extend(instruction_template_P1_query_response_default)
net_new.extend(instruction_template_P0_query_response_animal)

for instruction_processors in net_new:
    instruction_processors.add_processor(processors_query_response_p0_evaluator_openai)


# Query Response P0 and P1 processor
# the initial state to inject into the next processors (for template 01 question and answering)
p0_and_p1_response_processor = FullJoinStateProcessor(
    state=State(
        config=StateConfig(
            ersion="Draft Version 0.1",
            name="merged state animal query state template",
            output_path='../states/animallm/prod',
            output_primary_key_definition=[
                StateDataKeyDefinition(name='animal'),
                StateDataKeyDefinition(name='query')
            ],
            include_extra_from_input_definition=[
                StateDataKeyDefinition(name='query_template_id')
            ],
            template_columns=[
                StateDataKeyDefinition(name='query')
            ],
            remap_query_state_columns=[
                StateDataKeyDefinition(name='query_template', alias='query')
            ]
            ## TODO add join column key definitions or create a new processor type for joinable by keys
        )),
    processors=net_new
)


# Query Response P(n) Perspective Processor
pN_response_processor = FullJoinStateProcessor(
    state=State(

        config=StateConfig(
            version="Draft Version 0.1",
            name="merged state 2",
            output_path='../states/animallm/prod',
            output_primary_key_definition=[
                StateDataKeyDefinition(name='animal'),
                StateDataKeyDefinition(name='query')
            ],
            include_extra_from_input_definition=[
                StateDataKeyDefinition(name='query_template_id')
            ],
            template_columns=[
                StateDataKeyDefinition(name='query')
            ],
            remap_query_state_columns=[
                StateDataKeyDefinition(name='query_template', alias='query')
            ]
            ## TODO add join column key definitions
        )),
    processors=instruction_template_Pn_query_response_perspectives
)

for instruction_processors in instruction_template_Pn_query_response_perspectives:
    instruction_processors.add_processor(processors_query_response_p0_evaluator_openai)



# test function
if __name__ == '__main__':

    # process P0 and P1 query responses
    p0_and_p1_response_processor(states=[query_template_state, animal_state])
    pN_response_processor(states=[animal_state, query_template_state, perspective_definitions])

    exit(0)

    # merge the animal + query template + perspective datasets
    # process Pn perspective responses

    # print(animal_query_state_join_processor.build_final_output_path())
    # print_state_information('../states/animallm/devtest/')
    # processor_1 = animal_query_state_join_processor.processors[0]

    # processors_query_response_other_perspective

    # test_proc = processors[1]
    #
    # query_template_state = State.load_state('../dataset/animallm/query_template_state_devtest.json')
    # animal_state = State.load_state('../dataset/animallm/animal_state_devtest.json')



    pass