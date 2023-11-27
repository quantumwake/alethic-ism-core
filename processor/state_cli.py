import os

import utils
from processor.processor_question_answer import AnthropicQuestionAnswerProcessor
from processor.processor_state import State, StateDataColumnDefinition
import logging as log
from datetime import datetime as dt

logging = log.getLogger(__name__)


def print_state_information(path: str, recursive: bool = False, name_filter: str = None):

    if not os.path.exists(path):
        raise Exception(f'state path does not exist: {path}')

    files = os.listdir(path)

    if not files:
        logging.error(f'no state files found in {path}')
        return

    for nodes in files:

        full_path = f'{path}/{nodes}'
        if os.path.isdir(full_path):
            if recursive:
                logging.info(f'recursive path {full_path}')
                print_state_information(full_path)

            continue

        logging.info(f'----------------------------------------------------------------------')

        stat = os.stat(full_path)
        logging.debug(f'loading state file with path: {full_path}')
        state = State.load_state(full_path)
        show = not name_filter or name_filter.lower() in state.config.name.lower()

        if not show:
            continue

        configuration_string = "\n\t".join([f'{key}:{value}' for key, value in state.config.model_dump().items()])
        columns_string = ", ".join([f'[{key}]' for key in state.columns.keys()])
        logging.info(f'config: {configuration_string}')
        logging.info(f'columns: {columns_string}')
        logging.info(f'state row count: {utils.implicit_count_with_force_count(state)}')
        logging.info(f'created on: {dt.fromtimestamp(stat.st_ctime)}, '
                     f'updated on: {dt.fromtimestamp(stat.st_mtime)}, '
                     f'last access on: {dt.fromtimestamp(stat.st_atime)}')

def add_column_value_constant_to_state(column: StateDataColumnDefinition,
                                       state_file: str = None,
                                       state: State = None):
    if not state and not state_file:
        raise Exception(f'you must specify either a state_file or a load a state using the '
                        f'State.load_state(..) and pass it as a parameter')

    if state and state_file:
        raise Exception(f'cannot assign both state_file and state, choose one')

    if state_file:
        state = State.load_state(state_file)

    if column.name in state.columns:
        raise Exception(f'column {column.name} already exists in state with config: {state.config}')

    state.add_column(column)

    return state


if __name__ == '__main__':
    log.basicConfig(level="DEBUG")
    print_state_information('../states/animallm/prod', name_filter="P0")

    exit(0)
### **** IMPORTANT p(n)
    #anthropic
    state = State.load_state('../states/animallm/prod/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')
    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_provider_name",
            value=state.config.provider_name,
            data_type="str"
        ))

    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_model_name",
            value=state.config.model_name,
            data_type="str"
        ))
    state.save_state(state.config.output_path) ## persist the state again

    # openai
    state = State.load_state('../states/animallm/prod/49717023a01b090af5315b23ed38bef5143acb3887b54d9fd2155da18bd2144e.pickle')
    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_provider_name",
            value=state.config.provider_name,
            data_type="str"
        ))

    state = add_column_value_constant_to_state(
        state=state,
        column=StateDataColumnDefinition(
            name="response_model_name",
            value=state.config.model_name,
            data_type="str"
        ))
    state.save_state(state.config.output_path) ## persist the state again

    exit(0)


### ****** IMPORTANT (P0)
    ## need to aadd the following additional columns to the datasets

    # for all Model Names with ~ P0 (all model sources)
    # -- add perspective_index (P0)
    # anthropic
    # state = State.load_state('../states/animallm/prod/7c382cd88e9f4d3637fe301db76d8d93e1b86e2638b20d681de8171d703b3471.pickle')
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="perspective_index",
    #         value="P0",
    #         data_type="str"
    #     ))

    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_provider_name",
    #         value=state.config.provider_name,
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_model_name",
    #         value=state.config.model_name,
    #         data_type="str"
    #     ))
    #
    state.save_state(state.config.output_path) ## persist the state again

    exit(0)

    # openai
    # state = State.load_state('../states/animallm/prod/3554a988fc52a327f4f190a2c934ba6e95d489ca94172904224203b5748468d8.pickle')
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="perspective_index",
    #         value="P0",
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_provider_name",
    #         value=state.config.provider_name,
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_model_name",
    #         value=state.config.model_name,
    #         data_type="str"
    #     ))
    #
    # state.save_state(state.config.output_path) ## persist the state again

### ****** IMPORTANT (P1)
    # for P1 (all model sources)
    # -- perspective (default)
    # -- perspective_index (P1)

    # per model we need to add (for config.model_name = 'claude-2.1')
    # -- response_source_model (claude-2.1)
    # -- response_source_provider (anthropic)
    # state = State.load_state('../states/animallm/prod/0de579554936c55fde82826dbe0629965e050444a37a0cf0defa093a15fb27f7.pickle')
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="perspective_index",
    #         value="P1",
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="perspective",
    #         value="Default",
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_provider_name",
    #         value=state.config.provider_name,
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_model_name",
    #         value=state.config.model_name,
    #         data_type="str"
    #     ))
    # state.save_state(state.config.output_path) ## persist the state again


    # per model we need to add (for config.model_name = 'gpt-4-1106-preview')
    # -- response_source_model (claude-2.1)
    # -- response_source_provider (openai)
    #
    # state = State.load_state('../states/animallm/prod/441da549424d1243072613741c4e51bcfaa6bfdf436a72ee90da6f31b6bb5f19.pickle')
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="perspective_index",
    #         value="P1",
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="perspective",
    #         value="Default",
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_provider_name",
    #         value=state.config.provider_name,
    #         data_type="str"
    #     ))
    #
    # state = add_column_value_constant_to_state(
    #     state=state,
    #     column=StateDataColumnDefinition(
    #         name="response_model_name",
    #         value=state.config.model_name,
    #         data_type="str"
    #     ))
    # state.save_state(state.config.output_path)  ## persist the state again
    #



    # state = State.load_state('../states/animallm/prod/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')
    #
    # print(state.config.name)
    # print(state.config.output_path)
    # print(state.count)
    # state.config.name = 'AnimaLLM Instruction for Query Response Perspective P(n)'
    # state.config.output_path = '../states/animallm/prod'
    # state.save_state('../states/animallm/prod/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')
    #
    # p = AnthropicQuestionAnswerProcessor(state=state)
    # print(p.build_final_output_path())
    # state.save_state(p.build_final_output_path())


