from typing import List, Dict, Optional, Any

from langchain.callbacks.manager import CallbackManagerForChainRun
from langchain.chains import ConversationalRetrievalChain, ConversationChain


class ConversationalRetrievalChainCustom(ConversationChain):

    @property
    def input_keys(self) -> List[str]:
        """Input keys."""
        return ["question"]

