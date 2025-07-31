import json
import datetime as dt

from typing import Union, List

from ismcore.model.base_model import SessionMessage
from ismcore.model.processor_state import StateConfigLM, StateConfigStream
from ismcore.processor.base_processor import BaseProcessor
from ismcore.utils.general_utils import build_template_text_v2
from ismcore.utils.ism_logger import ism_logger

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

    async def _execute(self, user_prompt: str, system_prompt: str, values: dict | List[dict]):
        raise NotImplementedError(f'You must implement the _execute(..) method')

    async def process_input_data(self, input_data: dict | List[dict], force: bool = False):
        if not input_data:
            return []

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
            result, result_type, response_raw_data = await self._execute(
                user_prompt=user_prompt,
                system_prompt=system_prompt,
                values=input_data
            )

            # we build a new output state to be appended to the output states
            if self.config.flag_include_prompts_in_state:
                additional_query_state = {'user_prompt': user_prompt, 'system_prompt': system_prompt}
            else:
                additional_query_state = None

            return await self.finalize_result(result=result, input_data=input_data, additional_query_state=additional_query_state)
        except Exception as exception:
            await self.fail_execute_processor_state(
                # self.output_processor_state,
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_data
            )

    async def process_input_data_stream(self, input_data: dict | List[dict], force: bool = False):
        if not self.stream_route:
            raise ValueError(
                f"streams are not supported by provider: {self.output_processor_state.id}, "
                f"route_id {self.output_processor_state.id}")

        if not input_data:
            raise ValueError("invalid input state, cannot be empty")

        # if not isinstance(self.config, StateConfigStream):
        #     raise NotImplementedError()

        template = build_template_text_v2(self.template, input_data)

        # TODO this such a terrible HACK to use a session id for a given processor state stream
        if 'session_id' in input_data:
            session_id = input_data["session_id"]
            subject = f"processor.state.{self.output_state.id}.{session_id}"
        else:
            subject = f"processor.state.{self.output_state.id}"

        name = f"{subject}".replace("-", "_")

        # begin the processing of the prompts
        logging.debug(f"entered streaming mode, state_id: {self.output_state.id}")

        # submit to the fully qualified subject, which may include a session id
        stream_route = self.stream_route.clone(
            route_config_updates={
                "subject": subject,
                "name": name
            }
        )

        try:
            # submit the original request to the stream, such that it is broadcasted to all subscribers of the subject
            # TODO this needs to be invoked at the LM processor level, pre-stream-processing
            if 'source' in input_data:
                await stream_route.publish(input_data['source'])
                await stream_route.publish("<<>>SOURCE<<>>")

            if 'input' in input_data:
                await stream_route.publish(input_data['input'])
                await stream_route.publish("<<>>INPUT<<>>")

            # flush the stream to ensure the messages are sent to the stream server
            await stream_route.flush()

            # build a coroutine calling the concrete implementation of the stream
            stream = self._stream(input_data=input_data, template=template)

            # execute and iterate the yielded data directly into the upstream route
            async for content in stream:
                try:
                    if isinstance(content, str):
                        await stream_route.publish(content)
                        await stream_route.flush()
                    elif content is None:
                        # Log or handle the None case if necessary
                        logging.warning('Received NoneType content, skipping...')
                    else:
                        # Handle unexpected types
                        logging.warning(f'Unexpected content type: {type(content)}')
                except Exception as critical:
                    # Provide more detailed exception handling
                    logging.critical(f'Exception encountered during streaming: {critical}', exc_info=True)

            # TODO this needs to be invoked at the LM processor level, post-stream-processing
            # submit the response message to the stream.
            await stream_route.publish("<<>>ASSISTANT<<>>")
            await stream_route.flush()

            # should gracefully close the connection
            await stream_route.disconnect()

            logging.debug(f"exit streaming mode, state_id: {self.output_state.id}")
        except Exception as exception:
            # submit the response message to the stream.
            await stream_route.publish("<<>>ERROR<<>>")
            await stream_route.flush()
            await self.fail_execute_processor_state(
                # self.output_processor_state,
                route_id=self.output_processor_state.id,
                exception=exception,
                data=input_data
            )
