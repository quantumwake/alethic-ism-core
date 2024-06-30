from .processor_state import StateConfigLM
from .utils.ismlogging import ism_logger
from .base_processor import BaseProcessor
from .utils.general_utils import build_template_text

logging = ism_logger(__name__)


class BaseProcessorLM(BaseProcessor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def config(self) -> StateConfigLM:
        return self.output_state.config

    @property
    def user_template(self):
        if self.config.user_template_id:
            template = self.storage.fetch_template(self.config.user_template_id)
            return template.template_content
        return None

    @property
    def system_template(self):
        if self.config.system_template_id:
            template = self.storage.fetch_template(self.config.system_template_id)
            return template.template_content
        return None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ensure that the configuration passed is of StateConfigLM
        if not isinstance(self.output_state.config, StateConfigLM):
            raise ValueError(f'invalid state config, '
                             f'got {type(self.output_state.config)}, '
                             f'expected {StateConfigLM}')

    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        if not input_query_state:
            return

        # TODO maybe validate the input state to see if it was already processed for this particular output state?
        #
        # # create the input query state entry primary key hash string
        # input_query_state_key_hash, input_query_state_key_plain = (
        #   TODO this was the old way, needs to use the input state id's primary key not the output state's primary key.
        #       alternatively this should be handled at the state-router
        #   self.output_state.build_row_key_from_query_state(query_state=input_query_state)
        # )
        #
        # # skip processing of this query state entry if the key exists, unless forced to process
        # if self.has_query_state(query_state_key=input_query_state_key_hash, force=force):
        #     return

        # build final user and system prompts using the query state entry as the input data
        status, user_prompt = build_template_text(self.user_template, input_query_state)
        status, system_prompt = build_template_text(self.system_template, input_query_state)

        # begin the processing of the prompts
        try:

            # execute the underlying model function
            result, result_type, response_raw_data = (
                self._execute(
                    user_prompt=user_prompt,
                    system_prompt=system_prompt,
                    values=input_query_state
                )
            )

            # we build a new output state to be appended to the output states
            additional_query_state = {
                'user_prompt': user_prompt,
                'system_prompt': system_prompt,
            }

            return await self.finalize_result(
                input_query_state=input_query_state,
                result=result,
                additional_query_state=additional_query_state
            )

        except Exception as exception:
            await self.fail_execute_processor_state(
                # self.output_processor_state,
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_query_state
            )

