import logging
import os
from enum import Enum
from typing import List, Dict, Any

import utils
from processor.base_processor import BaseProcessor
from processor.base_question_answer_processor import StateConfigLM
from processor.processor_state import State, StateConfig, StateDataKeyDefinition
from processor.processors import anthropic_question_answer, openai_question_answer, openai_question_answer_multi_persona
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
        anthropic_question_answer,
        openai_question_answer,
        openai_question_answer_multi_persona
    ]
)

chain()
