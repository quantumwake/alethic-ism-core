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

def create_p0_processor_version(version):
    # version = 7
    version_dir = f'version0_{version}'
    version_text = f'Draft Version 0.{version}'
    template_file = f'../templates/animallm/{version_dir}/instruction_template_P0_query_response_animal_user_v{version}.json'
    output_path = f'../states/animallm/prod/{version_dir}/p0'

    # initialize a list of processors with the same initial configuration
    instruction_template_P0_query_response_animal = initialize_processors_with_same_state_config(
        config=StateConfigLM(
            name="AnimaLLM Instruction for P0 response",
            version="{version_text}",
            user_template_path=template_file,
            output_path=output_path,
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
        processor_types=[AnthropicQuestionAnswerProcessor, OpenAIQuestionAnswerProcessor])

    return instruction_template_P0_query_response_animal

instruction_template_P0_query_response_animal =create_p0_processor_version(6)

# # create multiple versions to run
# instruction_template_P0_query_response_animal = [processor for version in range(4,5)
#                                                  for processor
#                                                  in create_p0_processor_version(version)]

instruction_template_P1_query_response_default = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction for P1 response",
        version="Draft Version 0.2",
        system_template_path='../templates/animallm/version0_2/instruction_template_P1_query_response_default_perspective_system_v2.json',
        user_template_path='../templates/animallm/version0_2/instruction_template_P1_query_response_default_perspective_user_v2.json',
        output_path='../states/animallm/prod/version0_2',
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
    processor_types=[OpenAIQuestionAnswerProcessor])


# initialize a list of processors with the same initial configuration
instruction_template_Pn_query_response_perspectives = initialize_processors_with_same_state_config(
    config=StateConfigLM(
        name="AnimaLLM Instruction for Query Response Perspective P(n)",
        version="Draft Version 0.1",
        user_template_path='../templates/animallm/version0_1/instruction_template_P(n)_query_response_perspective.json',
        output_path='../states/animallm/prod/version0_1',
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
    processor_types=[OpenAIQuestionAnswerProcessor])
#

processors_query_response_p0_evaluator_openai = OpenAIQuestionAnswerProcessor(
    state=State(
        config=StateConfigLM(
            version="Draft Version 0.6",
            name="AnimaLLM Instruction for Query Response Evaluation P0 (versioned)",
            # the templates are not the same thing as the version of the run
            # (you can use a specific template but have multiple run versions)
            system_template_path='../templates/animallm/version0_4/instruction_template_P0_evaluator_system_v4.json',
            user_template_path='../templates/animallm/version0_4/instruction_template_P0_evaluator_user_v4.json',
            output_path='../states/animallm/prod/version0_6/p0_eval',
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
# net_new.extend(instruction_template_P1_query_response_default)
net_new.extend(instruction_template_P0_query_response_animal)

## TODO EVALUATOR of P0 and P1
# for instruction_processors in net_new:
#     instruction_processors.add_processor(processors_query_response_p0_evaluator_openai)

# Query Response P0 and P1 processor
# the initial state to inject into the next processors (for template 01 question and answering)
p0_and_p1_response_processor = FullJoinStateProcessor(
    state=State(
        config=StateConfig(
            ersion="Draft Version 0.4",
            name="initial input - merged states for animal with query template",
            output_path='../states/animallm/prod/version0_4/initial_input',
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
        )),
    processors=net_new
)


# P(n) -- Merged Animals (A) -> Template Queries (IT) -> P(n) Perspectives from P2 to P7
pN_response_processor = FullJoinStateProcessor(
    state=State(

        config=StateConfig(
            version="Draft Version 0.2",
            name="Merged Animals (A) -> Template Queries (IT) -> P(n) Perspectives from P2 to P7",
            output_path='../states/animallm/prod/version0_2',
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
        )),
    processors=instruction_template_Pn_query_response_perspectives
)

## TODO Evalutor of P(n)
# for instruction_processors in instruction_template_Pn_query_response_perspectives:
#     instruction_processors.add_processor(processors_query_response_p0_evaluator_openai)



# test function
if __name__ == '__main__':

    # process P0 and P1 query responses
    p0_and_p1_response_processor(states=[query_template_state, animal_state])
    # pN_response_processor(states=[animal_state, query_template_state, perspective_definitions])

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