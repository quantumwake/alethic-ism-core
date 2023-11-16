from typing import List

import utils
from processor.base_processor import BaseProcessor
from processor.question_answer_v2 import OpenAIQuestionAnswerProcessor, AnthropicQuestionAnswerProcessor



class ProcessorChain(BaseProcessor):

    def __init__(self, name: str, config: dict, processors: List[BaseProcessor]):
        super().__init__(name=name, config=config, processors=processors)


    def __call__(self, *args, **kwargs):
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
            input_processor.state = input_processor.load_state()

            # execute processor
            input_processor()

            # the output state file of the current processor run, goes into the
            output_state_file = input_processor.build_state_cache_filename()

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
                child_processor.input_state_filename = output_state_file
                child_processor.state_cache_path = processor.state_cache_path
                execute_processor_recursive(input_processor=child_processor, parent_processor=input_processor)

        # execute root processors
        for processor in self.processors:
            processor.state_cache_path = self.state_cache_path
            execute_processor_recursive(processor, self)


chain = ProcessorChain(
    name="AnimaLLM Test Evaluation Processor Chain",
    config={
        'state_cache_path': '../dataset/examples/states',
        'state_query_key_definition': ['query', 'context'],
        'input_file': '../dataset/examples/processor/vetted/questions/qa_4gs.json'
    },

    # First Processor - Q/A "Vetted" Questions
    processors=[
        OpenAIQuestionAnswerProcessor(
            name="[AnimaLLM]/[Evaluation]/[Human]/[Categorical]/[Question]",
            config={
                'input_file': '../dataset/examples/processor/vetted/questions/qa_4gs.json',
                'system_template_file': '../templates/questions/questions_with_context_system_template.json',
                'user_template_file': '../templates/questions/questions_with_context_user_template.json',
                'state_query_key_definition': ['query', 'context'],
                'output_include_states_details_from_input': [
                    {'query': 'input_query'},
                    {'context': 'input_context'}
                ]
            },

            # 1st Order Processor Output State => 2nd Order Processors
            processors=
            [
                # Input 1st Order Processor Output State => Question & Answer Persona Template
                OpenAIQuestionAnswerProcessor(
                    name="[AnimaLLM]/[Evaluation]/[OpenAI]/[Categorical]/[Question Response]/[Persona Response]",
                    config={
                        'system_template_file': None,
                        'user_template_file': '../templates/questions_with_persona/questions_with_persona_user_template.json',
                        'state_query_key_definition': ['input_query', 'input_query'],
                        'output_include_states_details_from_input': [
                            {'input_query': 'input_query'},
                            {'input_query': 'input_context'}
                        ]
                    }),

                # Input 1st Order Processor Output State => Question & Answer Perspective Template (OpenAI)
                OpenAIQuestionAnswerProcessor(
                    name="[AnimaLLM]/[Evaluation]/[OpenAI]/[Categorical]/[Question Response]/[Persona Evaluation]",
                    config={
                        'system_template_file': None,
                        'user_template_file': '../templates/perspectives/perspective_user_template_v3.json',
                        'state_query_key_definition': ['input_query', 'input_context'],
                        'output_include_states_details_from_input': [
                            {'input_query': 'input_query'},
                            {'input_context': 'input_context'},
                            {'input_response': 'default_response'}  # we take the response from the previous processor
                        ]
                    }),

                # Input 1st Order Processor Output State => Question & Answer Perspective Template (Anthropic)
                AnthropicQuestionAnswerProcessor(
                    name="[AnimaLLM]/[Evaluation]/[Anthropic]/[Categorical]/[Question Response]/[Persona Evaluation]",
                    config={
                        'system_template_file': None,
                        'user_template_file': '../templates/perspectives/perspective_user_template_v3.json',
                        'state_query_key_definition': ['input_query', 'input_context'],
                        'output_include_states_details_from_input': [
                            {'input_query': 'input_query'},
                            {'input_context': 'input_context'},
                            {'input_response': 'default_response'}  # we take the response from the previous processor
                        ]
                    })
            ]
        )
    ]
)

chain()