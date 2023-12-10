from typing import List
from alethic.processor.base_processor import BaseProcessor
from alethic.processor.processor_question_answer import OpenAIQuestionAnswerProcessor
from alethic.processor.processor_state import State, StateDataColumnDefinition, StateConfigLM, StateDataKeyDefinition

import random
import logging as log
import os

LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging = log.getLogger(__name__)
log.basicConfig(encoding='utf-8', level=LOG_LEVEL)


class ProcessorSimulator(BaseProcessor):

    def __init__(self, processors: List[BaseProcessor], runs: int = 20):
        # super().__init__(state=state, processors=processors)
        self.processors = processors
        self.random = False
        self.animals = {'dog', 'cat', 'cow', 'pig', 'chicken', 'mouse', 'shrimp'}
        self.runs = runs

    def __call__(self,
                 input_file: str = None,
                 input_state: State = None,
                 force_state_overwrite: bool = False,
                 *args, **kwargs):

        sample_indexes = []
        if self.random:
            sample_indexes = [random.randint(0, input_state.count-1) for i in range(50)]

        if self.animals:
            sample_indexes = [p1_states_v2.data['sample_no'][idx]
                              for idx, animal in enumerate(p1_states_v2.data['animal'].values)
                              if animal in self.animals]

        # fetch the query states
        queries = [p1_states_v2.get_query_state_from_row_index(random_idx)
                   for random_idx in sample_indexes]

        # first load the state of each processor, before adding query states to its thread queue
        for processor in self.processors:
            processor.load_previous_state()

        # generate query states, each with self.run count
        query_states = [{
            **query, **{"sample_no_run_no": sample_no_run_no}
        } for sample_no_run_no in range(self.runs)
            for query in queries]

        for processor in self.processors:
            processor.process_by_query_states(
                query_states=query_states)



p1_states_v2 = State.load_state(
    '../states/animallm/prod/version0_1/caf27a61761a23e7d621a11808bd919a5c9ef5add717e4cb504550fa38f3da9e.pickle')


# add the source provider and model name
p1_states_v2.add_column(
    column=StateDataColumnDefinition(
        name="response_model_name",
        value=p1_states_v2.config.model_name,
        data_type="str"
    )
)

p1_states_v2.add_column(
    column=StateDataColumnDefinition(
        name="response_provider_name",
        value=p1_states_v2.config.provider_name,
        data_type="str"
    )
)
p1_states_v2.add_column(
    column=StateDataColumnDefinition(
        name="perspective",
        value='Default',
        data_type="str"
    )
)
p1_states_v2.add_column(
    column=StateDataColumnDefinition(
        name="perspective_index",
        value='P1',
        data_type="str"
    )
)

## add the sample no such that we can add it to the query key
p1_states_v2.expand_columns(
    column=StateDataColumnDefinition(name="sample_no"),
    value_func=lambda row_index: row_index)

processors_query_response_p0_evaluator_openai = OpenAIQuestionAnswerProcessor(
    state=State(
        config=StateConfigLM(
            version="Draft Version 0.1",
            name="AnimaLLM Instruction for Query Response Evaluation P0 (simulation)",
            system_template_path='../../templates/animallm/version0_2/instruction_template_P0_evaluator_system_v2.json',
            user_template_path='../../templates/animallm/version0_2/instruction_template_P0_evaluator_user_v2.json',
            output_path='../../states/animallm/prod',
            output_primary_key_definition=[
                StateDataKeyDefinition(name="query", alias="query"),
                StateDataKeyDefinition(name="animal", alias="animal"),
                StateDataKeyDefinition(name="perspective_index", alias="perspective_index"),
                StateDataKeyDefinition(name='response_provider_name'),
                StateDataKeyDefinition(name='response_model_name'),
                StateDataKeyDefinition(name='sample_no'),           # for simulation (not really needed)
                StateDataKeyDefinition(name='sample_no_run_no')     # same question, run number
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
                StateDataKeyDefinition(name='sample_no'),
                StateDataKeyDefinition(name='sample_no_run_no'),
                StateDataKeyDefinition(name="query_template_id", alias="query_template_id")
            ]
        )
    )
)

simulator = ProcessorSimulator(
    processors=[processors_query_response_p0_evaluator_openai]
)
simulator(input_state=p1_states_v2)

