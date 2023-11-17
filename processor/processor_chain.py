import logging
import os
from enum import Enum
from typing import List, Dict, Any

import utils
from processor.base_processor import BaseProcessor
from processor.base_question_answer_processor import StateConfigLM
from processor.processor_state import State, StateConfig, StateDataKeyDefinition
from processor.question_answer_v2 import OpenAIQuestionAnswerProcessor, AnthropicQuestionAnswerProcessor


LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging.basicConfig(encoding='utf-8', level=LOG_LEVEL)

class ProcessorChain(BaseProcessor):

    def __init__(self, state: State, processors: List[BaseProcessor]):
        super().__init__(state=state, processors=processors)


    def __call__(self, *args, **kwargs):

        # since this is the first chain we set the state to the input state
        self.state = State.load_state(input_path=self.input_path)

        #
        def execute_processor_recursive(input_processor: BaseProcessor, parent_processor: BaseProcessor):
            if not isinstance(input_processor, BaseProcessor):
                raise Exception(f'Invalid processor type {input_processor}, expected '
                                f'class base type {type(input_processor)}')

            # # merge the current processors headers and configuration into the new
            # config = utils.merge_nested_dicts(input_processor.config, config)
            # header = utils.merge_nested_dicts(input_processor.header, header)
            #
            # input_processor.config = config.copy()
            # input_processor.header = header.copy()

            # map config onto input processor config map,
            # load the existing state into the input processor,
            # remap the config/header back on the loaded input state
            # input_processor.state = input_processor.load_state()

            # execute processor
            input_processor(input_state=parent_processor.state)

            # the output state file of the current processor run, goes into the
            output_state_file = input_processor.built_final_output_path()

            # if it has child processors
            if not input_processor.processors:
                return

            # iterate each child processor and inject the output state
            # of the current processor into each of the child processor
            for child_processor in input_processor.processors:
                # execute recursively
                if not isinstance(child_processor, BaseProcessor):
                    raise Exception(f'Invalid child processor {child_processor}, in input processor {input_processor}')

                # TODO we could probably use a queue such that it can be better distributed

                # execute recursively
                child_processor.output_path = proc.output_path
                child_processor.input_path = output_state_file
                execute_processor_recursive(input_processor=child_processor,
                                            parent_processor=input_processor)

        # checks if x is set, if so return otherwise returns o
        f = lambda x, o: x if x else o

        # execute root processors
        for proc in self.processors:
            proc.output_path = self.output_path
            proc.input_path = f(proc.input_path, self.input_path)
            proc.include_extra_from_input_definition = f(proc.include_extra_from_input_definition, self.include_extra_from_input_definition)
            proc.output_primary_key_definition = f(proc.output_primary_key_definition, self.output_primary_key_definition)
            execute_processor_recursive(proc, self)
#
# class StateQueryKeyDefinitionType(Enum):
#     RED = 1
#     GREEN = 2
#     BLUE = 3

# def do_something(data_entry: dict, input_state: dict)


a_a="""Here is a brief summary paragraph on places animals like to live in:

Animals like to live in habitats suited to their needs. Wild animals tend to prefer natural environments like forests, grasslands, wetlands, deserts etc. Domesticated animals are often kept as pets in homes or on farms. They like safe spaces with shelter, food, water and room to move around. The ideal home for an animal provides comfort, protects them from predators, and allows them to exhibit natural behaviors. With proper care, animals can thrive in a variety of environments.
"""

b_a="""Here is a brief summary paragraph responding to why some animals only live in certain places:

Some animals are only found living in certain places due to adaptations that help them survive in those particular environments. For example, polar bears are found in icy, Arctic regions because they have thick fur and insulating fat to withstand the cold temperatures. Penguins are found in Antarctica because they are excellent swimmers adapted to the cold waters. Camels survive in hot, dry deserts due to their ability to go long periods without water. The adaptations and abilities of different animal species determine where they are able to successfully live.
"""

c_a="""Here is a one paragraph summary response on farm animals:

Common farm animals include cows, pigs, chickens, sheep, goats, horses, and donkeys. These animals are raised on farms for meat, milk, eggs, wool, labor, transportation, recreation, and companionship. Cows provide beef and dairy products. Pigs provide pork. Chickens give eggs and meat. Sheep produce wool and meat. Goats offer milk and meat. Horses and donkeys are used for labor, transportation, and recreation on farms. A variety of farm animals are raised to provide food and other products for human use and enjoyment.
"""



a_b = utils.parse_response(a_a)
b_b = utils.parse_response(b_a)
c_b = utils.parse_response(c_a)



chain = ProcessorChain(
    state=State(
        config=StateConfig(
            name="AnimaLLM Test Evaluation Processor Chain",
            input_path='../dataset/examples/processor/vetted/questions/qa_4gs_v2.json',
            output_path='../dataset/examples/states',
            output_primary_key_definition=[
                StateDataKeyDefinition(name="query"),
                StateDataKeyDefinition(name="context")
            ]
        )
    ),

    # First Processor - Q/A "Vetted" Questions
    processors=[
        AnthropicQuestionAnswerProcessor(
            state=State(
                config=StateConfigLM(
                    name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
                    system_template_path='../templates/questions/questions_with_context_system_template.json',
                    user_template_path='../templates/questions/questions_with_context_user_template.json',
                    output_primary_key_definition=[
                        StateDataKeyDefinition(name="query"),
                        StateDataKeyDefinition(name="context")
                    ],
                    include_extra_from_input_definition=[
                        StateDataKeyDefinition(name="query", alias='input_query'),
                        StateDataKeyDefinition(name="context", alias='input_context')
                    ]
                )
            )
        ),

        OpenAIQuestionAnswerProcessor(
            state=State(
                config=StateConfigLM(
                    name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
                    system_template_path='../templates/questions/questions_with_context_system_template.json',
                    user_template_path='../templates/questions/questions_with_context_user_template.json',
                    output_primary_key_definition=[
                        StateDataKeyDefinition(name="query"),
                        StateDataKeyDefinition(name="context")
                    ],
                    include_extra_from_input_definition=[
                        StateDataKeyDefinition(name="query", alias='input_query'),
                        StateDataKeyDefinition(name="context", alias='input_context')
                    ]
                )
            ),
        ),
    ]
)

chain()

anthropic_2 = AnthropicQuestionAnswerProcessor(
            state=State(
                config=StateConfigLM(
                    name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
                    model_name="claude",
                    system_template_path='../templates/questions/questions_with_context_system_template.json',
                    user_template_path='../templates/questions/questions_with_context_user_template.json',
                    output_primary_key_definition=[
                        StateDataKeyDefinition(name="query"),
                        StateDataKeyDefinition(name="context")
                    ],
                    include_extra_from_input_definition=[
                        StateDataKeyDefinition(name="query", alias='input_query'),
                        StateDataKeyDefinition(name="context", alias='input_context')
                    ]
                )
            ),
        ),

processors = [
                OpenAIQuestionAnswerProcessor(
                    state=State(
                        config=StateConfigLM(
                            name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
                            system_template_path='../templates/perspectives/questions_with_context_system_template.json',
                            user_template_path='../templates/perspectives/questions_with_context_user_template.json',
                            output_primary_key_definition=[
                                StateDataKeyDefinition(name="query"),
                                StateDataKeyDefinition(name="context")
                            ],
                            include_extra_from_input_definition=[
                                StateDataKeyDefinition(name="query", alias='input_query'),
                                StateDataKeyDefinition(name="context", alias='input_context')
                            ]
                        )
                    )
                )
            ]