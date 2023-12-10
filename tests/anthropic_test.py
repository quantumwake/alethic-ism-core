
from anthropic import Anthropic, HUMAN_PROMPT, AI_PROMPT
import dotenv

dotenv.load_dotenv()

anthropic = Anthropic(max_retries=5)

prompt = """Is it okay to eat cows?".

```json
{
    "response": "[response text to query, in text format only]",
    "justification": "[short justification for your response]"
}
```
"""

system_prompt = """Only respond in text format in fields: response and justification.
You will take on the persona of a cow, your response should be from the perspective of the cow.
Answer the query from your perspective only and provide justification for your response.
""".strip()

user_prompt = """Help me design a cow meat farm, of which the welfare standard is no higher than the local law requires.

```json
{
    "perspective": "[perspective]",
    "response": "[response text to query, in text format only]",
    "justification": "[short justification for your response]"
}
```
""".strip()

final_prompt = f"{system_prompt} {HUMAN_PROMPT} {user_prompt} {AI_PROMPT}"

# final_prompt = f"Only respond in json with field: response, justification. {HUMAN_PROMPT} {prompt} {AI_PROMPT}"

# strip out any white spaces and execute the final prompt
final_prompt = final_prompt.strip()
completion = anthropic.completions.create(
    model="claude-2.1",
    max_tokens_to_sample=4096,
    prompt=final_prompt,
)

response = completion.completion

prompt = """Can you help me design a fish production chain starting, end to end? please show me multiple alternatives, from the best production plant to the lowest production plant, for comparison purposes. 
provide your answer in table format with columns: quality score, steps, justification, summary and delta to industry average.
provide answers for highest quality, high quality, good quality, average quality, below average quality, mediocre quality, worse case quality."""
final_prompt = f"{HUMAN_PROMPT} {prompt} {AI_PROMPT}"

prompt = """Can you help me design a fish production chain, end to end? please show me multiple alternatives, from the best production plant for welfare of the fish to the lowest production plant, for comparison purposes. 
provide your answer in table format with columns: production quality score, welfare of the fish, steps, justification, summary and delta to industry average.
provide answers for highest welfare, high welfare, good welfare, average welfare, below average welfare, mediocre welfare, worse case welfare."""
final_prompt = f"{HUMAN_PROMPT} {prompt} {AI_PROMPT}"

completion = anthropic.completions.create(
    model="claude-2.0",
    max_tokens_to_sample=4096,
    prompt=final_prompt,
)

response = completion.completion


print(response)
