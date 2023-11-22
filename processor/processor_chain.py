import logging
import os
from enum import Enum
from typing import List, Dict, Any

import utils
from processor.base_processor import BaseProcessor
from processor.base_question_answer_processor import StateConfigLM
from processor.processor_state import State, StateConfig, StateDataKeyDefinition
from processor.processors import anthropic_question_answer, openai_question_answer, \
    openai_question_answer_multi_persona, openai_question_answer_devtestset
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
            output_state_file = input_processor.build_final_output_path()

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





class ProcessorChainV2(BaseProcessor):

    def handle(self, processor: BaseProcessor, input_state: State):
        # TODO must return some sort of state update? or maybe not just needs a healthcheck
        processor(input_state=input_state)

    def __call__(self, input_state: State, *args, **kwargs):
        if not self.processors:
            raise Exception(f'no downstream processors available to propagate input query state. '
                            f'current config processor: {self.config}')

        for processor_node in self.processors:
            logging.debug(f'processing input state {input_state.config}')
            func = utils.higher_order_routine(self.handle,
                                              processor=processor_node,
                                              input_state=input_state)
            self.manager.add_to_queue(func)

        # wait for the completion of all processors or some exit code?
        # TODO should be running a health check on this
        self.manager.wait_for_completion()

    def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        # TODO handle this in a distributed way
        #  we could handle each individual state separately
        #  but for now we are simply passing the entire state as an input
        super().process_input_data_entry()
        pass




chain = ProcessorChainV2(
    state=State(
        config=StateConfig(
            name="AnimaLLM Evaluation Processor Chain",
            output_path='../states',
            output_primary_key_definition=[
                StateDataKeyDefinition(name="query"),
                StateDataKeyDefinition(name="context")
            ]
        )
    ),

    # First Processor - Q/A "Vetted" Questions
    processors=[
        # anthropic_question_answer,
        # openai_question_answer,
        openai_question_answer_devtestset
        # openai_question_answer_multi_persona
    ]
)


if __name__ == '__main__':
    initial_input_state_file ='../dataset/examples/processor/vetted/questions/qa_4gs_v2_devtestset.json'
    initial_input_state = State.load_state(initial_input_state_file)

    chain(input_state=initial_input_state)
