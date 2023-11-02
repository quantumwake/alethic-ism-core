from typing import Callable, Any

from langchain.callbacks import StreamingStdOutCallbackHandler
from langchain.schema import LLMResult


class StreamingAnnotatorCallbackHandler(StreamingStdOutCallbackHandler):

    def __init__(self,
                 on_new_token: Callable[[str], None] = None,
                 on_end_token: Callable[[LLMResult], None] = None):
        self.on_new_token_callback = on_new_token
        self.on_end_token_callback = on_end_token

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> None:
        """Run when LLM ends running."""
        self.on_end_token_callback(response)

    def on_llm_error(self, error: BaseException, **kwargs: Any) -> None:
        """Run when LLM errors."""

    def on_llm_new_token(self, token: str, **kwargs: Any) -> None:
        """Run on new LLM token. Only available when streaming is enabled."""
        self.on_new_token_callback(token)
