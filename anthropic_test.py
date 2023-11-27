
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

final_prompt = f"Only respond in json with field: response, justification. {HUMAN_PROMPT} {prompt} {AI_PROMPT}"

# strip out any white spaces and execute the final prompt
final_prompt = final_prompt.strip()
completion = anthropic.completions.create(
    model="claude-2",
    max_tokens_to_sample=4096,
    prompt=final_prompt,
)

response = completion.completion

prompt = "can you explain why?"
final_prompt = f"{HUMAN_PROMPT} {prompt} {AI_PROMPT}"

completion = anthropic.completions.create(
    model="claude-2",
    max_tokens_to_sample=4096,
    prompt=final_prompt,
)

response = completion.completion


print(response)
