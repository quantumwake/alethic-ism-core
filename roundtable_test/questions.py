import random
from typing import List

import openai
from langchain.prompts import ChatPromptTemplate, HumanMessagePromptTemplate, PromptTemplate
from langchain.schema import SystemMessage
from tenacity import retry, wait_exponential

chat_template = ChatPromptTemplate.from_messages(
    [
        SystemMessage(
            content=(
                """
                In a room sit three great philosophers: John Locke, Martin Heidegger, and Friedrich Nietzsche. 
                They begin to discuss the nature of existence, freedom, and society.
                
                Chat history:
                {history}
                
                """.strip().replace('\n', '')
            )
        ),

        HumanMessagePromptTemplate.from_template("speaker: {speaker}, asks: {question}"),
    ]
)

TEST_TEMPLATE = """In a room sit three great philosophers: John Locke, Martin Heidegger, and Friedrich Nietzsche. They begin to discuss the nature of existence, freedom, and society.

Relevant pieces of previous conversation:
{history}


(You do not need to use these pieces of information if not relevant)


Current conversation:
Human: {input}
AI:"""
TEST_PROMPT = PromptTemplate(
   input_variables=["history", "input"], template=TEST_TEMPLATE
)


#
# chat_prompt = PromptTemplate(
#     template=chat_template, input_variables=["speaker", "question"]
# )


def new_prompt(speaker: str, question: str, previous_answer: str = None):
    # Initialize the first question
    if not previous_answer:
        question = f"what question comes to your mind to kick off this discussion?"

    question_prompt = chat_template.format_messages(
        speaker=speaker,
        question=question)

    # question_prompt = f"Persona: {philosopher_speaking}, {question}"
    return question_prompt


# Philosophers in the conversation
philosophers = ["John Locke", "Martin Heidegger", "Friedrich Nietzsche"]


@retry(wait=wait_exponential(multiplier=1, min=2, max=6))
def execute_initial_question(qa_chain, chats: List[dict]):

    # Initialize the first question
    speaker = random.choice(philosophers)
    question = f"what question comes to your mind to kick off this discussion?"

    question_prompt = new_prompt(
        speaker=speaker,
        question=question)

    # execute the chain
    result = qa_chain.predict(input=question_prompt)
    answer = result['answer']
    chats.append({
        "speaker": speaker,
        "question": question,
        "answer": answer
    })
    return question_prompt, answer


@retry(wait=wait_exponential(multiplier=1, min=2, max=6))
def execute_question_answer(qa_chain):
    # Initialize the first question
    speaker = random.choice(philosophers)
    question = f"what question comes to your mind to kick off this discussion?"

    question_prompt = new_prompt(
        speaker=speaker,
        question=question)

    # execute the chain
    result = qa_chain(question_prompt)
    answer = result['answer']
    chats.append({
        "speaker": speaker,
        "question": question,
        "answer": answer
    })
    return question_prompt, answer


@retry(wait=wait_exponential(multiplier=1, min=2, max=6))
def execute_chat_summarizer(chats: List[dict], vectordb):

    # Summarize all chats and store in vector db.
    previous_dialog = "\n".join(chats)
    summary_response = openai.ChatCompletion.create(
        model='gpt-4',
        messages=[
            {"role": "system", "content": "You are a summarizer."},
            {"role": "user", "content": f"Summarize in detail the conversation for the following discussions: {previous_dialog}"}
        ]
    )
    summarized_text = summary_response['choices'][0]['message']['content']

    # add the summarized text to the embedding space
    vectordb.add_texts(summarized_text, metadatas=[{"type": "summary"}])