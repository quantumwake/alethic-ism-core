# Examples & Use Cases

## 1. Building an LLM Processor

Every LLM processor (OpenAI, Anthropic, Gemini, etc.) follows the same pattern:

### Step 1: Implement the Processor

```python
from ismcore.processor.base_processor_lm import BaseProcessorLM
from ismcore.processor.monitored_processor_state import MonitoredUsage
from ismcore.utils.ism_logger import ism_logger

logging = ism_logger(__name__)

class MyLLMProcessor(BaseProcessorLM, MonitoredUsage):
    """Process entries through your LLM API."""

    def execute_entry(self, query_state: dict, **kwargs):
        # 1. Build prompts from templates
        messages = self.derive_messages(query_state=query_state)

        # 2. Call your LLM API
        response = my_llm_client.chat(
            messages=messages,
            temperature=self.properties.temperature,
            max_tokens=self.properties.maxTokens,
        )

        # 3. Track usage
        self.send_usage_input_tokens(response.input_tokens)
        self.send_usage_output_tokens(response.output_tokens)

        # 4. Return result dict — keys become output state columns
        return {"response": response.text}
```

### Step 2: Wire Up the Consumer

```python
from ismcore.messaging.base_message_consumer_processor import BaseMessageConsumerProcessor
from ismcore.messaging.base_message_router import Router
from ismcore.messaging.nats_message_provider import NATSMessageProvider
from ismcore.processor.base_processor import StatePropagationProviderRouterStateSyncStore

class MyConsumer(BaseMessageConsumerProcessor):
    def create_processor(self, **kwargs):
        return MyLLMProcessor(**kwargs)

# In main.py:
provider = NATSMessageProvider()
router = Router(provider=provider, config_path="routing-nats.yaml")
route = router.get_route("my/processor/selector")
monitor_route = router.get_route("processor/monitor")

consumer = MyConsumer(route=route, monitor_route=monitor_route)
consumer.start_consumer()
```

### Step 3: Configure NATS Routes

```yaml
# routing-nats.yaml
messageConfig:
  routes:
    - name: MY_LLM_PROCESSOR
      url: nats://localhost:4222
      subject: processor.my.llm
      selector: my/processor/selector
      queue: my_llm_queue
      ack_wait: 120
      batch_size: 1
      jetstream_enabled: true

    - name: PROCESSOR_MONITOR
      url: nats://localhost:4222
      subject: processor.monitor
      selector: processor/monitor
      jetstream_enabled: true
```

---

## 2. Working with States

### Creating a State with Columns

```python
from ismcore.model.processor_state import (
    State, StateConfig, StateDataColumnDefinition, StateDataKeyDefinition
)

state = State(
    id="my-state-001",
    project_id="project-123",
    config=StateConfig(
        primary_key=[
            StateDataKeyDefinition(name="user_id", required=True)
        ],
        flag_dedup_drop_enabled=True,
        flag_flatten_on_save=True,
    ),
    columns={},
    data={},
    mapping={},
    count=0,
)
```

### Applying Data to a State

```python
# Single entry
entry = {"user_id": "u1", "question": "What is AI?", "response": "..."}
state.apply_query_state(query_state=entry)

# State auto-discovers columns and indexes by primary key
print(state.count)       # 1
print(state.columns)     # {"user_id": ..., "question": ..., "response": ...}
```

### State with Inheritance

```python
# Output state inherits input columns
output_config = StateConfig(
    query_state_inheritance=[
        StateDataKeyDefinition(name="user_id"),
        StateDataKeyDefinition(name="question"),
    ],
    flag_query_state_inheritance_all=False,
)

# Or inherit everything:
output_config = StateConfig(
    flag_query_state_inheritance_all=True,
)
```

### Column Remapping

```python
config = StateConfig(
    remap_query_state_columns={
        "old_column_name": "new_column_name",
        "input_text": "prompt",
    }
)
```

---

## 3. Using the Filter System

```python
from ismcore.model.filter import Filter, FilterItem, FilterOperator

f = Filter()

# Exact match
f.add_filter(FilterItem(key="status", operator=FilterOperator.EQ, value="active"))

# Range
f.add_filter(FilterItem(
    key="score",
    operator=FilterOperator.BETWEEN,
    value=0.5,
    secondary_value=1.0,
))

# String contains
f.add_filter(FilterItem(
    key="response",
    operator=FilterOperator.CONTAINS,
    value="artificial intelligence",
))

# Nested key (dot notation)
f.add_filter(FilterItem(
    key="context.run_id",
    operator=FilterOperator.EQ,
    value="run-abc",
))

# Apply to data
data = [{"status": "active", "score": 0.8, "response": "..."}]
filtered = f.apply_filter_on_data(data)
```

---

## 4. Safe Code Execution (Processor-Python)

```python
from ismcore.compiler.runnable import BaseRunnable

runnable = BaseRunnable(storage=my_storage, state=my_state, properties={})

# User-provided code (sandboxed via RestrictedPython)
code = '''
class Runnable(BaseSecureRunnable):
    def process(self, query_states):
        results = []
        for qs in query_states:
            text = qs.get("input_text", "")
            results.append({"word_count": len(text.split())})
        return results
'''

instance = runnable.instantiate(code)
results = instance.process([{"input_text": "hello world"}])
# [{"word_count": 2}]
```

### Safe Expression Evaluation

```python
from ismcore.utils.evaluate import safer_evaluate

# Expressions run in a restricted sandbox
result = safer_evaluate("sum(range(10))")          # 45
result = safer_evaluate("hashit('my_string')")     # SHA256 hash
result = safer_evaluate("x + y", allowed_vars={"x": 1, "y": 2})  # 3
```

---

## 5. Flattening Nested Data

```python
from ismcore.utils.map_utils import flatten

# Nested dict → dot-notation keys
data = {
    "user": {"name": "Alice", "age": 30},
    "scores": [{"test": "A", "val": 95}, {"test": "B", "val": 87}]
}

result = flatten(data)
# [
#   {"user.name": "Alice", "user.age": 30, "scores.test": "A", "scores.val": 95},
#   {"user.name": "Alice", "user.age": 30, "scores.test": "B", "scores.val": 87},
# ]
```

---

## 6. Custom State Router Consumer

The state router uses `BaseMessageConsumer` directly (not `BaseMessageConsumerProcessor`):

```python
from ismcore.messaging.base_message_provider import BaseMessageConsumer

class MyRouterConsumer(BaseMessageConsumer):
    async def execute(self, route, message, data):
        msg_type = data.get("type")

        if msg_type == "query_state_entry":
            # State-triggered: look up route, forward to processor
            route_id = data["route_id"]
            # ... resolve processor, build outbound message, publish

        elif msg_type == "query_processor_entry":
            # Direct trigger: forward to specific processor
            processor_id = data["processor_id"]
            # ... resolve processor, publish
```

---

## 7. Usage Tracking

```python
from ismcore.model.base_model_usage_and_limits import Usage, UnitType, UnitSubType

# Record usage
usage = Usage(
    resource_id="processor-123",
    resource_type="processor",
    unit_type=UnitType.TOKEN,
    unit_sub_type=UnitSubType.INPUT,
    unit_count=1500,
)
```

---

## 8. Processor Status Validation

```python
from ismcore.utils.state_utils import validate_processor_status_change
from ismcore.model.base_model import ProcessorStatusCode

# Valid transition
validate_processor_status_change(
    ProcessorStatusCode.QUEUED,
    ProcessorStatusCode.RUNNING,
)  # OK

# Invalid transition — raises exception
validate_processor_status_change(
    ProcessorStatusCode.COMPLETED,
    ProcessorStatusCode.RUNNING,
)  # Raises!
```

---

## 9. Template Rendering

```python
from ismcore.utils.general_utils import build_template_text_v2

template = "Hello ${user_name}, your score is ${score}."
query_state = {"user_name": "Alice", "score": 95}

rendered = build_template_text_v2(template, query_state)
# "Hello Alice, your score is 95."
```

---

## 10. Batch Route Consumer (NATSRouteBatch)

For high-throughput consumers that group messages by route:

```python
from ismcore.messaging.nats_message_route_batch import NATSRouteBatch

# NATSRouteBatch fetches multiple messages at once,
# groups them by route_id, and delivers batched
# query_states to a callback per group.
# Used by alethic-ism-state-sync-store for efficient DB inserts.
```
