import os

import openai
import random
import json
import logging

from langchain.callbacks.manager import CallbackManager
from langchain.chains import RetrievalQA, LLMChain, ConversationalRetrievalChain, ConversationChain
from langchain.chains.chat_vector_db.prompts import CONDENSE_QUESTION_PROMPT
from langchain.chains.conversational_retrieval.prompts import QA_PROMPT
from langchain.chains.question_answering import load_qa_chain
from langchain.chat_models import ChatOpenAI
from langchain.embeddings import HuggingFaceInstructEmbeddings, OpenAIEmbeddings
from langchain.llms.huggingface_pipeline import HuggingFacePipeline
from langchain.llms.openai import OpenAI
from langchain.memory import VectorStoreRetrieverMemory
from langchain.prompts import PromptTemplate, ChatPromptTemplate, HumanMessagePromptTemplate
from langchain.schema import LLMResult, SystemMessage
from langchain.vectorstores.pgvector import PGVector

from ConversationalRetrievalChainCustom import ConversationalRetrievalChainCustom
from callback import StreamingAnnotatorCallbackHandler
from questions import new_prompt, execute_initial_question, chat_template, TEST_PROMPT

# Initialize logging
logging.basicConfig(level=logging.DEBUG)

import dotenv
# philosophers = {}
# with open('philosophers.json', 'r') as f:
#     philosophers = json.load(f)

# Replace "your-openai-api-key-here" with your actual OpenAI API key
api_key = os.environ.get("OPENAI_API_KEY", None)


# Initialize OpenAI GPT-4 client
openai.api_key = api_key
os.environ['OPENAI_API_KEY'] = api_key

# Initialize the GPT engine
gpt_engine_id = "gpt-4"  # Replace this with the GPT-4 engine ID when available

# Initialize the conversation
conversation = [
    {"role": "user",
     "content": "In a room sit three great philosophers: John Locke, Martin Heidegger, and Friedrich Nietzsche. They begin to discuss the nature of existence, freedom, and society."}
]

# Philosophers in the conversation
philosophers = ["John Locke", "Martin Heidegger", "Friedrich Nietzsche"]

# Maximum number of dialogue turns (optional)
max_turns = 10

# use an embedding to give the philosophers context on the discussion
# embeddings = HuggingFaceInstructEmbeddings(
#     model_name="hkunlp/instructor-large", model_kwargs={"device": DEVICE}
# )

embeddings = OpenAIEmbeddings()
COLLECTION_NAME = "philosopher_discussion"

pguser = 'colab'
pgpass = 'a094459fc15df838'
pghost = '134.209.82.17'
pgport = 5432
pgdb = 'colab'

CONNECTION_STRING = f"postgresql://{pguser}:{pgpass}@{pghost}:{pgport}/{pgdb}"
COLLECTION_NAME = "philosophy_dialog"

chats = []


def _on_end_token(self, response: LLMResult) -> None:
    print(f'llm response: {response}')
    data = self.data
    chats.append(data)


def _on_new_token(self, token) -> None:
    self.data = f'{self.data}{token}'


# setup an empty vector database and initialize the collection
vectordb = PGVector(
    embedding_function=embeddings,
    pre_delete_collection=True,
    collection_name=COLLECTION_NAME,
    connection_string=CONNECTION_STRING
)

# setup the callback handler upon new token or end token
callback_result_handler = StreamingAnnotatorCallbackHandler(
    on_new_token=_on_new_token,
    on_end_token=_on_end_token
)

# this is used for retrieval from pgvector
# async streaming llm for the vectordb
# streaming_llm = ChatOpenAI(
#     streaming=True,
#     callback_manager=CallbackManager([callback_result_handler]),
#     verbose=True,
#     temperature=0
# )

# setup the document retrieval question and answer llm
# document_chain = load_qa_chain(streaming_llm, chain_type="stuff", prompt=QA_PROMPT)

# setup the language module llm
llm = OpenAI(model_name='gpt-4', temperature=0.1, request_timeout=300)
# llm = HuggingFacePipeline(pipeline=text_pipeline, model_kwargs={"temperature": 0})

# the non-streaming LLM for questions
# question_generator = LLMChain(llm=llm, prompt=chat_template)
# logging.debug(f'configured question generator llm with {question_generator}')

# initialize ConversationalRetrievalChain chabot
logging.debug(f'setting up conversational retrieval chain')
# qa_chain = ConversationalRetrievalChainCustom(
#     retriever=vectordb.as_retriever(),
#     combine_docs_chain=document_chain,
#     question_generator=question_generator,
#     get_chat_history=lambda x: "")

memory = VectorStoreRetrieverMemory(retriever=vectordb.as_retriever(k=2))
qa_chain = ConversationChain(
    llm=llm,
    prompt=TEST_PROMPT,
    memory=memory,
    verbose=True
)

logging.debug(f'configured  conversational retrieval chain {qa_chain}')


# kick off initial question
question_prompt, question_answer = execute_initial_question(qa_chain, chats)


# Start the dialogue loop
for turn in range(max_turns):

    # select a new persona at random, whom will ask the next question
    speaker = random.choice(philosophers)

    question_prompt = chat_template.format_messages(
        speaker=speaker,
        question=new_question)

    # execute the chain
    result = qa_chain(question_prompt)

    answer = result['answer']
    chats.append(question_prompt)

    # create a unique message key
    # execute the chain and fetch the results
    key = str(random.randbytes(5))
    logging.info(f'executing qa_chain for question {question_prompt} with key {key}')
    result = qa_chain(question_prompt)

    logging.info(f'completed execution of qa_chain {key}')
    new_question = result["answer"]





