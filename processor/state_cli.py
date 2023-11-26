import os

from processor.processor_question_answer import AnthropicQuestionAnswerProcessor
from processor.processor_state import State
import logging as log

logging = log.getLogger(__name__)


def print_state_information(path: str, recursive: bool = False):

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

        logging.info(f'processing state file with path: {full_path}')

        state = State.load_state(full_path)

        # if isinstance(state.config, StateConfigLM):

        configuration_string = "\n\t".join([f'{key}:{value}' for key, value in state.config.model_dump().items()])
        columns_string = ", ".join([f'[{key}]' for key in state.columns.keys()])
        logging.info(f'config: {configuration_string}')
        logging.info(f'columns: {columns_string}')
        logging.info(f'state row count: {utils.implicit_count_with_force_count(state)}')
        logging.info(f'created on: {dt.fromtimestamp(stat.st_ctime)}, '
                     f'updated on: {dt.fromtimestamp(stat.st_mtime)}, '
                     f'last access on: {dt.fromtimestamp(stat.st_atime)}')

if __name__ == '__main__':
    log.basicConfig(level="DEBUG")
    # print_state_information('../states/animallm/prod')

    state = State.load_state('../states/animallm/prod/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')

    print(state.config.name)
    print(state.config.output_path)
    print(state.count)
    # state.config.name = 'AnimaLLM Instruction for Query Response Perspective P(n)'
    # state.config.output_path = '../states/animallm/prod'
    # state.save_state('../states/animallm/prod/570fb94c8609ef5d6915b6580041bc5afcefd460ac7f50da601600c07613048a.pickle')
    #
    # p = AnthropicQuestionAnswerProcessor(state=state)
    # print(p.build_final_output_path())
    # state.save_state(p.build_final_output_path())


