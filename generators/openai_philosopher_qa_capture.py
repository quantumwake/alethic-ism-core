import os.path
from typing import List

import openai
import json

from datasets import load_dataset

import dotenv

# Replace "your-openai-api-key-here" with your actual OpenAI API key
api_key = os.environ.get("OPENAI_API_KEY", None)

# Initialize OpenAI GPT-4 client
openai.api_key = api_key

# Expanded list of philosophers with importance rank and philosophical domain
philosophers = {
    "Plato": {"importance_rank": 1, "domain": "Metaphysics"},
    "Aristotle": {"importance_rank": 2, "domain": "Ethics"},
    "Immanuel Kant": {"importance_rank": 3, "domain": "Epistemology"},
    "Descartes": {"importance_rank": 4, "domain": "Epistemology"},
    "Socrates": {"importance_rank": 5, "domain": "Ethics"},
    "John Locke": {"importance_rank": 6, "domain": "Political Philosophy"},
    "Thomas Hobbes": {"importance_rank": 7, "domain": "Political Philosophy"},
    "Nietzsche": {"importance_rank": 8, "domain": "Existentialism"},
    "Jean-Jacques Rousseau": {"importance_rank": 9, "domain": "Political Philosophy"},
    "Bertrand Russell": {"importance_rank": 10, "domain": "Analytic Philosophy"},
    "Søren Kierkegaard": {"importance_rank": 11, "domain": "Existentialism"},
    "Martin Heidegger": {"importance_rank": 12, "domain": "Existentialism"},
    "Edmund Husserl": {"importance_rank": 13, "domain": "Phenomenology"},  # Heidegger's Professor
    "Hannah Arendt": {"importance_rank": 14, "domain": "Political Philosophy"},  # Heidegger's Pupil
    "David Hume": {"importance_rank": 15, "domain": "Empiricism"},
    "Spinoza": {"importance_rank": 16, "domain": "Metaphysics"},
    "Confucius": {"importance_rank": 17, "domain": "Ethics"},
    "Friedrich Hegel": {"importance_rank": 18, "domain": "Metaphysics"},
    "Ludwig Wittgenstein": {"importance_rank": 19, "domain": "Analytic Philosophy"},
    "John Stuart Mill": {"importance_rank": 20, "domain": "Utilitarianism"},
}

philosophical_questions = [
    'What is the Meaning of Life?',
    'What is Morality?',
    'What is Truth?',
    'What is Freedom?',
    'What is Justice?',
    'Is God Dead?',
    'What is Happiness?',
    'What is Suffering?',
    'What is Knowledge?',
    'What is Reality?',
    'What is Time?',
    'What is Mind?',
    'What is Self?',
    'What is Death?',
    'What is Existence?',
    'What is Duty?',
    'What is Virtue?',
    'What is Evil?',
    'What is Consciousness?',
    'What is Art?',
    'What is Gender?',
    'What is Nature?',
    'What is Perception?',
    'What is War?',
    'What is Peace?',
    'What is Friendship?',
    'What is Loneliness?',
    'What is Intelligence?',
    'What is Technology?',
    'What is Democracy?',
    'What is Education?',
    'What is History?',
    'What is Science?',
    'What is Culture?',
    'What is Language?',
    'What is Identity?',
    'What is Love?',
    'What is Wealth?',
    'What is Poverty?',
]

models = openai.Model.list()
print(f'available models: {models}')

PATH_QA_FILE = f'philosophical_questions.json'
PATH_QA_FILE_CSV = f'philosophical_questions.csv'

def load_philosphical_questions(file: str):

    if not os.path.exists(file):
        return {}

    # Dictionary to hold responses
    with open(file, 'r') as f:
        return json.load(f)


def generate_questions_and_answers_json():
    # load prexisting dataset, if any
    response_dict = load_philosphical_questions(PATH_QA_FILE)
    for philosopher, info in philosophers.items():

        question_answers = []

        # if the philosopher dictionary key already exists
        if philosopher in response_dict:
            # fetch the questions and answers dictionary for the philosopher
            philosopher_dict = response_dict[philosopher]

            # check to see if it has a QA field, if it does then set the questions
            # answers array for the philosopher such that we can continue to append
            # to it instead of redoing all the questions/answers
            if philosopher_dict and 'qa' in philosopher_dict:
                question_answers = philosopher_dict['qa']
            else:
                question_answers = []

        print(f'Questions from Philosopher {philosopher}')
        # iterate through each question and check to make sure it has
        # not already been processed, if it has then skip it.
        for question in philosophical_questions:

            # if the questions and answers is available then
            # check to see if the question and answer already exists
            find_question_answer = [x for x in question_answers if x['question'] == question]

            if find_question_answer:
                answer = find_question_answer[0]['answer'].strip().replace('\n', '')
                print(
                    f' -- skipping question and answer for philosopher {philosopher} question {question} already exists')
                print(f' --- * answer: {answer}')
                continue

            print(f' -- {question}')
            # Formulate the question for GPT-4
            system = (f"You are {philosopher} philosopher, respond to the question below "
                      "as if you are having a dialog with another person. Only answer"
                      "questions that are your philosophical work, or beliefs, do not"
                      "answer untruthfully. Do not make up stuff if you do not know."
                      "If you do not have an answer, skip the question and say pass.")

            # execute the open ai api
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {
                        "role": "system",
                        "content": system
                    },
                    {
                        "role": "user",
                        "content": f"{question}"
                    }
                ],
                temperature=0.8,
                max_tokens=1024
            )

            # Capture the relevant text
            try:
                answer = response.choices[0]['message']['content'].strip().replace('\n', '')
                question_answers.append({"question": question, "answer": answer})
                print(f' --- * answer: {answer}')
            except Exception as e:
                answer = f'error {e}'
                print(f'critical error handling question {question} for philosopher {philosopher} ')

            # Save the list of question-answers and info in the dictionary
            response_dict[philosopher] = {
                "importance_rank": info["importance_rank"],
                "domain": info["domain"],
                "qa": question_answers
            }

            with open('philosophical_questions.json', 'w') as f:
                json.dump(response_dict, f)

    # Print or otherwise use the 'response_dict'
    return response_dict


def convert_to_csv():
    qa_dict = load_philosphical_questions(PATH_QA_FILE)

    if os.path.exists(PATH_QA_FILE_CSV):
        os.remove(PATH_QA_FILE_CSV)

    with open(PATH_QA_FILE_CSV, 'w') as f:
        f.write(f'PHILOSOPHER, QUESTION, ANSWER\n')

        for philosopher, data in qa_dict.items():
            print(f'philosopher: {philosopher}')
            questions_answers = data['qa']

            for qa in questions_answers:
                question = qa["question"].strip()
                answer = qa["answer"].strip().replace('\n', ' ').replace('"', "'")
                f.write(f'"{philosopher}", "{question}", "{answer}"\n')
                # f.write('\n')


print('do something useful here, either run the convert_to_csv() function or the generate_questions_and_answers_json() function')
# convert_to_csv()
# generate_questions_and_answers_json()