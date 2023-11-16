import logging
import re
from typing import List, Any, Type

import os.path
import json
import openai
import pandas as pd
import hashlib
import dotenv

from anthropic import Anthropic, HUMAN_PROMPT, AI_PROMPT
from tenacity import retry, wait_exponential, wait_random

import utils
from processor import map_flattener
from processor.base_processor import BaseProcessor
from processor.base_question_answer_processor import BaseQuestionAnswerProcessor

dotenv.load_dotenv()
openai_api_key = os.environ.get('OPENAI_API_KEY', None)
openai.api_key = openai_api_key

LOG_LEVEL = os.environ.get("LOG_LEVEL", "DEBUG").upper()
LOG_PATH = os.environ.get("LOG_PATH", "../logs/")
LOG_FILE = f'{LOG_PATH}/examples.log'

logging.basicConfig(filename=LOG_FILE, encoding='utf-8', level=LOG_LEVEL)


class AnthropicBaseProcessor(BaseQuestionAnswerProcessor):

    def __init__(self, name: str, config: dict, processors: List[BaseProcessor] = None, *args, **kwargs):
        new_config = {**{'provider_name': "Anthropic", 'model_name': "claude-2"}, **config}
        super().__init__(name=name, config=new_config, processors=processors, **kwargs)

        self.anthropic = Anthropic(max_retries=5)

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
        data, columns = self._parse_response(response=response)
        return data


class OpenAIBaseProcessor(BaseQuestionAnswerProcessor):

    def __init__(self, name: str, config: dict, processors: List[BaseProcessor] = None, *args, **kwargs):
        new_config = {**{'provider_name': "OpenAI", 'model_name': "gpt-4-1106-preview"}, **config}
        super().__init__(name=name, config=new_config, processors=processors, **kwargs)

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
        return self._parse_response(response=response)

