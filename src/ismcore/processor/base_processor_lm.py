import json
import datetime as dt

from typing import List

from ismcore.model.base_model import SessionMessage, ProcessorPropertiesLM
from ismcore.model.processor_state import StateConfigLM, OutputEnrichment
from ismcore.processor.base_processor import BaseProcessor
from ismcore.utils.general_utils import build_template_text_v2
from ismcore.utils.ism_logger import ism_logger

logging = ism_logger(__name__)

class BaseProcessorLM(BaseProcessor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ensure that the configuration passed is of StateConfigLM
        if not isinstance(self.output_state.config, StateConfigLM):
            raise ValueError(f'invalid state config, '
                             f'got {type(self.output_state.config)}, '
                             f'expected {StateConfigLM}')

    @property
    def properties(self) -> ProcessorPropertiesLM:
        """Override base class to return typed LM properties"""
        if not self.processor.properties:
            return ProcessorPropertiesLM()
        return ProcessorPropertiesLM(**self.processor.properties)

    @property
    def config(self) -> StateConfigLM:
        return self.output_state.config

    @property
    def user_template(self):
        if self.config.user_template_id:
            template = self.storage.fetch_template(self.config.user_template_id)
            return template

        return None

    @property
    def system_template(self):
        if self.config.system_template_id:
            template = self.storage.fetch_template(self.config.system_template_id)
            return template

        return None

    def derive_messages(self, user_prompt: str, system_prompt: str = None) -> list[dict]:
        """Build the base messages list for an LLM API call."""
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt.strip()})
        if user_prompt:
            messages.append({"role": "user", "content": user_prompt.strip()})
        return messages

    def derive_messages_with_session_data_if_any(
        self, user_prompt: str, system_prompt: str = None, input_data: any = None
    ) -> list[dict]:
        """Build messages list, prepending session history if available.

        Ordering: system prompt -> session history -> current user prompt.
        """
        current_messages = self.derive_messages(user_prompt=user_prompt, system_prompt=system_prompt)

        if not isinstance(input_data, dict):
            return current_messages
        if not {'session_id', 'source', 'input'}.issubset(input_data.keys()):
            return current_messages

        session_messages = self.fetch_session_data(input_data)
        if not session_messages:
            return current_messages

        history = [{"role": msg['role'], "content": msg['content']} for msg in session_messages]

        # system first, then history, then current user prompt
        result = [msg for msg in current_messages if msg['role'] == 'system']
        result.extend(history)
        result.extend(msg for msg in current_messages if msg['role'] != 'system')
        return result

    def update_session_data(self, input_data: any, input_template: str, output_data: str):
        if not isinstance(input_data, dict):
            return

        if 'session_id' not in input_data:
            return

        user_id = input_data['source']
        session_id = input_data['session_id']

        # session message: original user text + rendered prompt that was executed
        self.storage.insert_session_message(SessionMessage(
            user_id=user_id,
            session_id=session_id,
            original_content=json.dumps({"role": "user", "content": input_data['input']}),
            executed_content=json.dumps({"role": "user", "content": input_template}),
            message_date=dt.datetime.utcnow()
        ))

        # session message: assistant-generated response
        self.storage.insert_session_message(SessionMessage(
            user_id=user_id,
            session_id=session_id,
            original_content=json.dumps({"role": "assistant", "content": output_data}),
            executed_content=None,
            message_date=dt.datetime.utcnow()
        ))

    async def process_input_data_stream(self, input_data: dict | List[dict]):
        """LM streaming: resolve templates, emit chat markers, yield model chunks."""
        user_prompt = build_template_text_v2(self.user_template, input_data)
        system_prompt = build_template_text_v2(self.system_template, input_data) if self.system_template else None

        if 'source' in input_data:
            yield input_data['source']
            yield "<<>>SOURCE<<>>"

        if 'input' in input_data:
            yield input_data['input']
            yield "<<>>INPUT<<>>"

        async for chunk in self.stream_llm(user_prompt=user_prompt, system_prompt=system_prompt, values=input_data):
            yield chunk

        yield "<<>>ASSISTANT<<>>"

    async def stream_llm(self, user_prompt: str, system_prompt: str, values: dict | List[dict]):
        """Yield raw LLM response chunks. Override in concrete LLM processors."""
        raise NotImplementedError("LLM streaming not implemented")

    async def execute_llm(self, user_prompt: str, system_prompt: str, values: dict | List[dict]) \
            -> tuple[dict | List[dict] | None, str, any]:
        """Execute the underlying model call. Must be implemented by subclasses.

        Returns:
            tuple of (parsed_output, output_type, raw_output)
                - parsed_output: the processed result as a dict or list of dicts, or None on failure
                - output_type: string identifier for the output format (e.g. 'json', 'text', 'csv', 'binary', etc.)
                - raw_output: the unmodified response from the model
        """
        raise NotImplementedError(f'You must implement the execute_llm(..) method')


    async def process_input_data(self, input_data: dict | List[dict], force: bool = False)\
            -> tuple[dict | List[any] | None, any]:

        if not input_data:
            return [], None

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
        user_prompt = build_template_text_v2(self.user_template, input_data)
        system_prompt = build_template_text_v2(self.system_template, input_data) if self.system_template else None

        # begin the processing of the prompts
        try:
            # execute the underlying model function
            response, response_type, response_raw = await self.execute_llm(
                user_prompt=user_prompt,
                system_prompt=system_prompt,
                values=input_data
            )

            # we build a new output state to be appended to the output states
            additional_query_state = None
            props = self.output_state.typed_properties
            if props.output and props.output.enrichments and OutputEnrichment.PROMPTS in props.output.enrichments:
                additional_query_state = {'user_prompt': user_prompt, 'system_prompt': system_prompt}

            # finalize the output by performing any necessary post-processing, such as updating the query state entry with the result,
            #  and return the finalized output, along with the original raw response data from the upstream processor implementation (if applicable)
            finalized_output = await self.finalize_result(
                result=response,
                input_data=input_data,
                additional_query_state=additional_query_state,
                raw_output=response_raw
            )
            return finalized_output, response_raw

        except Exception as exception:
            await self.fail_execute_processor_state(
                # self.output_processor_state,
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_data
            )
            return None, None
