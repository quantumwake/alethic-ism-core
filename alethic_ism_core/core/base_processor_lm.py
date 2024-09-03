import json
import datetime as dt

from typing import Union
from .base_model import SessionMessage
from .processor_state import StateConfigLM, StateConfigStream
from .utils.ismlogging import ism_logger
from .base_processor import BaseProcessor
from .utils.general_utils import build_template_text, build_template_text_v2

logging = ism_logger(__name__)


class BaseProcessorLM(BaseProcessor):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # ensure that the configuration passed is of StateConfigLM
        if (not isinstance(self.output_state.config, StateConfigLM) and not
                isinstance(self.output_state.config, StateConfigStream)):

            raise ValueError(f'invalid state config, '
                             f'got {type(self.output_state.config)}, '
                             f'expected {StateConfigLM} or {StateConfigStream}')

    @property
    def config(self) -> Union[StateConfigLM, StateConfigStream]:
        return self.output_state.config

    @property
    def user_template(self):
        if not isinstance(self.config, StateConfigLM):
            raise ValueError("system template cannot be set for streaming configuration, use template instead")

        if self.config.user_template_id:
            template = self.storage.fetch_template(self.config.user_template_id)
            return template

        return None

    @property
    def system_template(self):
        if not isinstance(self.config, StateConfigLM):
            raise ValueError("system template cannot be set for streaming configuration, use template instead")

        if self.config.system_template_id:
            template = self.storage.fetch_template(self.config.system_template_id)
            return template

        return None

    def derive_messages(self, template):
        return [{
            "role": "user",
            "content": template
        }]

    def derive_messages_with_session_data_if_any(self, template: str, input_data: any):
        if not isinstance(input_data, dict):
            return self.derive_messages(template=template)

        if not set(['session_id', 'source', 'input']).issubset(input_data.keys()):
            return self.derive_messages(template=template)

        message_list = self.fetch_session_data(input_data)
        if message_list:
            message_list = [
                {"role": msg['role'], "content": msg['content']}
                for msg in message_list
            ]

        message_list.extend(self.derive_messages(template=template))
        return message_list

    # def update_session_data_entry(self, session_id: str, session_entry: dict):
    #     if not session_id:
    #         return
    #
    #     # the following elements need to exist for sessions to works correctly
    #     if not set(['source', 'role', 'input']).issubset(session_entry.keys()):
    #         raise ValueError(f"source, role and input need to be present for sessions to work correctly in dictionary {session_entry}")
    #
    #
    #     # store the message for later retrieval by a process (during it's execution phase for this input)
    #     # self.storage.insert_session_message(session_id, json.dumps(session_entry))

    def update_session_data(self, input_data: any, input_template: str, output_data: str):
        if not isinstance(input_data, dict):
            return

        if 'session_id' not in input_data:
            return

        user_id = input_data['source']
        session_id = input_data['session_id']

        # self.update_session_data_entry(session_id=session_id, session_entry={
        #     "source": input_data['source'] if 'source' in input_data else "user",
        #     "role": "user",  # TODO?
        #     "input": input_data['input'] if 'input' in input_data else input_template,
        #     "content": input_template,  # the rendered template (given input_data) as executed by processor
        # })

        # session message object representing the original text that came in from the user and
        # the prompt that was actually executed (as per instruction template for StateConfig -> self.config)
        self.storage.insert_session_message(SessionMessage(
            user_id=user_id,
            session_id=session_id,
            original_content=json.dumps({"role": "user", "content": input_data['input']}),
            executed_content=json.dumps({"role": "user", "content": input_template}),
            message_date=dt.datetime.utcnow()
        ))

        # session message representing the assistant generated text, given the user executed content (as per above)
        # the prompt that was actually executed (as per instruction template for StateConfig -> self.config)
        self.storage.insert_session_message(SessionMessage(
            user_id=user_id,
            session_id=session_id,
            original_content=json.dumps({"role": "assistant", "content": input_template}),
            executed_content=None,
            message_date=dt.datetime.utcnow()
        ))
        #
        # self.update_session_data_entry(session_id=session_id, session_entry={
        #     "role": "assistant",
        #     "source": input_data['source'] if 'source' in input_data else "assistant",
        #     "content": output_data,
        #     "input": input_template
        # })

    async def _execute(self, user_prompt: str, system_prompt: str, values: dict):
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
        user_prompt = build_template_text_v2(self.user_template, input_query_state)
        system_template = self.system_template
        system_prompt = build_template_text_v2(system_template, input_query_state) if system_template else None

        # begin the processing of the prompts
        try:
            # input_query_state = {
            #     key:value
            #     for key, value in input_query_state.items()
            #     if not key[:2] == "__" and not key[:-2] == '__'
            # }

            # execute the underlying model function
            result, result_type, response_raw_data = (
                await self._execute(
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
