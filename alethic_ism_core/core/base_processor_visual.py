from .processor_state import StateConfigVisual
from .base_processor import BaseProcessor
from .utils.general_utils import build_template_text
from .utils.ismlogging import ism_logger

logging = ism_logger(__name__)


class BaseProcessorVisual(BaseProcessor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @property
    def config(self) -> StateConfigVisual:
        return self.output_state.config

    @property
    def template(self):
        if self.config.template_id:
            template = self.storage.fetch_template(self.config.template_id)
            return template.template_content
        return None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ensure that the configuration passed is of StateConfigLM
        if not isinstance(self.output_state.config, StateConfigVisual):
            raise ValueError(f'invalid state config, '
                             f'got {type(self.output_state.config)}, '
                             f'expected {StateConfigVisual}')

    def _execute(self, template: str, values: dict):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    async def process_input_data_entry(self, input_query_state: dict, force: bool = False):
        if not input_query_state:
            return

        # build final user and system prompts using the query state entry as the input data
        status, template = build_template_text(self.template, input_query_state)

        # begin the processing of the prompts
        try:

            # execute the underlying model function
            result, result_type, response_raw_data = (
                self._execute(
                    template=template,
                    values=input_query_state
                )
            )

            # we build a new output state to be appended to the output states
            additional_query_state = {
                'template': template,
            }

            return await self.finalize_result(
                input_query_state=input_query_state,
                result=result,
                additional_query_state=additional_query_state
            )

        except Exception as exception:
            await self.fail_execute_processor_state(
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_query_state
            )
