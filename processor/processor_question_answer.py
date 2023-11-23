import logging as log
from typing import List

import os.path
import openai
import dotenv

from anthropic import Anthropic, HUMAN_PROMPT, AI_PROMPT
from tenacity import retry, wait_exponential, wait_random

import utils
from processor.base_processor import BaseProcessor
from processor.base_question_answer_processor import BaseQuestionAnswerProcessor
from processor.processor_state import State


dotenv.load_dotenv()
openai_api_key = os.environ.get('OPENAI_API_KEY', None)
openai.api_key = openai_api_key

logging = log.getLogger(__name__)


class AnthropicBaseProcessor(BaseQuestionAnswerProcessor):

    def __init__(self, state: State, processors: List[BaseProcessor] = None, *args, **kwargs):
        super().__init__(state=state, processors=processors, **kwargs)
        self.provider_name = self.config.provider_name if self.config.provider_name else "Anthropic"
        self.model_name = self.config.model_name if self.config.model_name else "claude-2"
        self.anthropic = Anthropic(max_retries=5)

        logging.info(f'extended instruction state machine: {type(self)} with config {self.config}')

    def batching(self, questions: List[str]):
        raise NotImplementedError()

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2))
    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        # add a system message if one exists
        final_prompt = f"{HUMAN_PROMPT} {user_prompt} {AI_PROMPT}"
        if system_prompt:
            final_prompt = f'{system_prompt} {final_prompt}'

        # strip out any white spaces and execute the final prompt
        final_prompt = final_prompt.strip()
        completion = self.anthropic.completions.create(
            model="claude-2",
            max_tokens_to_sample=4096,
            prompt=final_prompt,
        )

        response = completion.completion
        return response


class AnthropicQuestionAnswerProcessor(AnthropicBaseProcessor):

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2))
    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        response = super()._execute(user_prompt=user_prompt, system_prompt=system_prompt, values=values)
        return utils.parse_response_strip_assistant_message(response=response)


class OpenAIBaseProcessor(BaseQuestionAnswerProcessor):

    def __init__(self, state: State, processors: List[BaseProcessor] = None, *args, **kwargs):
        super().__init__(state=state, processors=processors, **kwargs)
        self.provider_name = self.config.provider_name if self.config.provider_name else "OpenAI"
        self.model_name = self.config.model_name if self.config.model_name else "gpt-4-1106-preview"

        logging.info(f'extended instruction state machine: {type(self)} with config {self.config}')

    def batching(self, questions: List[str]):
        raise NotImplementedError()

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2))
    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        # otherwise process the question
        messages_dict = []

        if user_prompt:
            user_prompt = user_prompt.strip()
            messages_dict.append({
                "role": "user",
                "content": f"{user_prompt}"
            })

        if system_prompt:
            system_prompt = system_prompt.strip()
            messages_dict.append({
                "role": "system",
                "content": system_prompt
            })

        if not messages_dict:
            raise Exception(f'no prompts specified for values {values}')

        # execute the open ai api function and wait for the response
        response = openai.ChatCompletion.create(
            model=self.model_name,
            messages=messages_dict,
            temperature=0.1,
            # TODO - IMPORTANT test this as it will likely have an impact on how the system responds
            max_tokens=4096
        )

        # final raw response, without stripping or splitting
        return response.choices[0]['message']['content']


class OpenAIQuestionAnswerProcessor(OpenAIBaseProcessor):

    @retry(wait=wait_exponential(multiplier=1, min=4, max=10) + wait_random(0, 2))
    def _execute(self, user_prompt: str, system_prompt: str, values: dict):
        response = super()._execute(user_prompt=user_prompt, system_prompt=system_prompt, values=values)

        return utils.parse_response(response=response)

