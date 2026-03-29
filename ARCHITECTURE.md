# Architecture — alethic-ism-core

## Role in the Ecosystem

`alethic-ism-core` is the shared foundation for all Python-based ISM services. It is a **library**, not a standalone service. Every Python ISM service depends on it.

```
                        ┌───────────────────────────┐
                        │     alethic-ism-core       │
                        │                           │
                        │  Models   Messaging       │
                        │  Storage  Processors      │
                        │  Utils    Compiler        │
                        │  Vault    Embedding       │
                        └─────────┬─────────────────┘
                                  │
              ┌───────────────────┼───────────────────────┐
              │                   │                       │
    ┌─────────▼──────┐  ┌────────▼────────┐   ┌──────────▼────────┐
    │  alethic-ism-db│  │ state-router    │   │ processor-*       │
    │  (Postgres     │  │ (msg dispatch)  │   │ (openai, anthropic│
    │   concrete     │  │                 │   │  gemini, python,  │
    │   storage)     │  │                 │   │  ollama, rag ...) │
    └────────────────┘  └─────────────────┘   └───────────────────┘
              │                                         │
    ┌─────────▼──────┐                        ┌─────────▼─────────┐
    │ state-sync-    │                        │ ism-api           │
    │ store          │                        │ (FastAPI REST)    │
    │ (persist state)│                        │                   │
    └────────────────┘                        └───────────────────┘
```

### Go Equivalent

`alethic-ism-core-go` provides parallel functionality for Go services:

| Python (this repo)             | Go (`alethic-ism-core-go`)        |
|--------------------------------|-----------------------------------|
| `ismcore.model.*`              | `pkg/data/models`                 |
| `ismcore.messaging.*`          | `pkg/routing/nats`                |
| `ismcore.storage.*`            | `pkg/repository/*`                |
| `ismcore.utils.evaluate`       | _(not implemented)_               |
| `ismcore.compiler.*`           | _(not implemented)_               |
| `ismcore.embedding.*`          | `pkg/repository/embedding`        |
| `ismcore.vault.*`              | `pkg/repository/vault`            |
| _(not implemented)_            | `pkg/cache` (TTL local cache)     |
| _(not implemented)_            | `pkg/s3` (S3/Spaces client)       |
| _(not implemented)_            | `pkg/auth` (JWT parsing)          |
| _(not implemented)_            | `pkg/crypto` (AES-256-GCM)        |
| _(not implemented)_            | `pkg/windowing` (block storage)   |

---

## Module Architecture

### 1. Model Layer

The model layer defines all data structures shared across the system. Everything is Pydantic `BaseModel`.

#### Core Enums

```
ProcessorStateDirection     INPUT | OUTPUT
ProcessorStatusCode         CREATED → QUEUED → RUNNING → COMPLETED | FAILED
                            Also: ROUTE, ROUTED, TERMINATE, STOPPED
EdgeFunctionType            CALIBRATOR | VALIDATOR | TRANSFORMER | FILTER
ConcurrencyMode             PROJECT_ID | USER_ID | ROUTE_ID | EXPRESSION
UsageUnitType               TOKEN
UserSessionAccessLevel      DEFAULT | ADMIN
StateStorageClass           FILE | DATABASE
VaultType                   VAULT | LOCAL | KMS
ConfigMapType               SECRET | CONFIG_MAP
```

#### Status State Machine

```
CREATED ──► QUEUED ──► RUNNING ──► COMPLETED
   │           │          │            │
   │           │          ▼            ▼
   │           │       STOPPED      FAILED
   │           │          │
   │           ▼          ▼
   └──────► TERMINATE ──► FAILED
```

Valid transitions enforced by `utils/state_utils.py`:

| From        | Allowed To                                    |
|-------------|-----------------------------------------------|
| CREATED     | QUEUED, RUNNING, TERMINATE, STOPPED, FAILED   |
| QUEUED      | STOPPED, TERMINATE, RUNNING, QUEUED            |
| RUNNING     | RUNNING, STOPPED, TERMINATE, FAILED, COMPLETED|
| STOPPED     | STOPPED, TERMINATE, FAILED                    |
| TERMINATE   | TERMINATE, FAILED                              |
| FAILED      | FAILED                                         |
| COMPLETED   | COMPLETED, FAILED                              |

#### Entity Relationship

```
UserProfile ──1:N──► UserProject ──1:N──► Processor
                                              │
                                          1:N │
                                              ▼
                                        ProcessorState ◄──── EdgeFunctionConfig
                                         (route)
                                           │
                                       N:1 │
                                           ▼
                                         State
                                           │
                                   ┌───────┼───────┐
                                   ▼       ▼       ▼
                              columns    data    mapping
                              (schema)  (rows)   (index)

Processor ──N:1──► ProcessorProvider
Processor ──► properties (JSON: ProcessorPropertiesBase/LM)
```

#### State Configuration Hierarchy

```
BaseStateConfig
  ├── StateConfig                  # Standard processing state
  │     ├── StateConfigLM          # + LLM templates (user/system)
  │     ├── StateConfigDB          # Database-sourced state
  │     ├── StateConfigCode        # Code execution state
  │     ├── StateConfigVisual      # Image/visual processing
  │     ├── StateConfigAudio       # Audio processing
  │     ├── StateConfigUserInput   # User input state
  │     └── StateConfigStream      # Streaming output state
  └── (flag_* fields on BaseStateConfig)
```

---

### 2. State Data Model

A `State` stores columnar data with indexing:

```
State
├── id: str
├── project_id: str
├── config: BaseStateConfig (polymorphic)
├── columns: Dict[name, StateDataColumnDefinition]
│     └── name, data_type, required, callable, dimensions, value, source
├── data: Dict[name, StateDataRowColumnData]
│     └── values: List[Any], count: int
├── mapping: Dict[row_key, StateDataColumnIndex]
│     └── indices: List[int]  (row positions for this key)
└── count: int
```

#### State Application Flow

```
apply_query_state(entry)
  │
  ├── pre_state_apply()
  │     ├── clean column names (DDL-safe)
  │     ├── remap columns (remap_query_state_columns)
  │     ├── apply template variables
  │     └── flatten nested structures (if flag_flatten_on_save)
  │
  ├── pre_state_apply_callable_and_constant_columns()
  │     └── evaluate callable columns via safer_evaluate()
  │
  ├── post_state_primary_key_apply()
  │     └── SHA256 hash of primary key values
  │
  ├── process_and_add_columns()
  │     └── auto-detect new columns from data
  │
  ├── process_and_add_row_data()
  │     └── append values, backfill missing columns
  │
  ├── process_and_add_row_data_mapping()
  │     └── index by primary key hash
  │
  └── post_state_apply()
        └── increment count
```

#### Result Merge Flow

```
apply_result(result, input_data, additional)
  │
  ├── if flag_query_state_inheritance_all:
  │     └── merge ALL input columns into result
  │
  ├── elif query_state_inheritance defined:
  │     └── apply_query_state_inheritance()
  │           └── selectively copy named keys
  │
  └── merge with processor result
        └── return [output_dict, ...]
```

---

### 3. Messaging Layer

NATS JetStream-based pub/sub with pull consumers.

#### Route Class Hierarchy

```
BaseRoute (abstract)
  ├── NATSRoute                    # Standard JetStream pull consumer
  ├── NATSRouteConcurrent          # Semaphore-controlled parallel workers
  └── NATSRouteBatch               # Batch fetch + group by route_id
```

#### Consumer Class Hierarchy

```
MonitoredUsage                     # Usage token tracking
  └── MonitoredProcessorState      # Status update publishing
        └── BaseMessageConsumer    # Core consumer loop
              └── BaseMessageConsumerProcessor  # Processor-specific logic
```

#### Consumer Loop

```
start_consumer()
  ├── setup_shutdown_signal()  (SIGTERM handler)
  ├── route.connect()
  ├── route.subscribe()
  └── consumer_loop()
        └── while RUNNING:
              ├── route.consume(wait=True)
              ├── for msg in messages:
              │     └── on_receive(msg)
              │           ├── JSON decode
              │           ├── pre_execute()    → status QUEUED
              │           ├── intra_execute()  → status RUNNING
              │           ├── execute(msg)     → process data
              │           ├── post_execute()   → status COMPLETED
              │           └── route.ack(msg)
              └── (loop continues)
```

#### Message Format

```json
{
  "type": "query_state",
  "route_id": "<processor_state.id>",
  "processor_id": "<processor.id>",
  "input_route_id": "<for retry/calibration>",
  "context": { "run_id": "...", "session_id": "..." },
  "query_state": [
    { "col_a": "val1", "col_b": "val2" },
    { "col_a": "val3", "col_b": "val4" }
  ]
}
```

#### NATS Route Configuration

```yaml
messageConfig:
  routes:
    - name: ROUTE_NAME
      url: nats://host:4222
      subject: processor.state.router
      selector: processor/state/router
      queue: state_router_queue
      ack_wait: 90           # seconds
      batch_size: 1
      jetstream_enabled: true
      concurrent_enabled: false
      concurrent_max_workers: 10
      concurrent_priority_enabled: false
      concurrent_max_workers_high: 5
```

---

### 4. Processor Layer

#### Execution Pipeline

```
BaseMessageConsumerProcessor.execute(message)
  │
  ├── validate type == "query_state"
  ├── extract route_id / processor_id
  ├── fetch_processor_state_outputs()
  │     └── load processor, provider, output states
  │
  ├── for each output state:
  │     ├── create_processor()  (abstract — impl provides LLM/code/etc.)
  │     │
  │     ├── if flag_enable_execute_set:
  │     │     └── processor.execute_set(query_states)
  │     │           └── batch processing
  │     │
  │     └── else: for each entry in query_state:
  │           └── processor.execute_entry(entry)
  │                 ├── apply_query_state(entry)  → dedup, inherit, template
  │                 ├── processor.execute()        → actual work (LLM call, etc.)
  │                 ├── apply_result()             → merge output + inheritance
  │                 └── propagate()                → publish to downstream
  │
  └── publish status to monitor route
```

#### Processor Class Hierarchy

```
BaseProcessor
  ├── BaseProcessorLM              # Language model processors
  │     ├── derive_messages()      # Build prompt from templates
  │     ├── derive_messages_with_session_data_if_any()
  │     ├── update_session_data()  # Persist conversation
  │     └── fetch_session_data()   # Load conversation history
  │
  └── BaseProcessorVisual          # Image/visual processors
```

#### State Propagation Strategies

```
StatePropagationProvider (abstract)
  │
  ├── StatePropagationProviderRouter
  │     └── Publishes to NATS: {subject}.{state_id}
  │     │
  │     ├── StatePropagationProviderRouterStateRouter
  │     │     └── Saves state, THEN routes to downstream processors
  │     │
  │     └── StatePropagationProviderRouterStateSyncStore
  │           └── Saves state via sync store route
  │
  ├── StatePropagationProviderCore
  │     └── Applies state locally (no routing)
  │
  ├── StatePropagationProviderEdgeFunction
  │     └── Runs edge functions (calibration, validation, transform, filter)
  │
  └── StatePropagationProviderDistributor
        └── Fans out to multiple propagation providers
```

---

### 5. Storage Layer

All storage is **interface-based**. This repo defines abstract classes; `alethic-ism-db` provides the PostgreSQL implementations.

#### Storage Interface Map

```
StateMachineStorage (composite)
  ├── UserProfileStorage           # Users, credentials
  ├── UserProjectStorage           # Projects (CRUD, soft delete)
  ├── TemplateStorage              # Instruction templates
  ├── WorkflowStorage              # UI nodes and edges
  ├── ProcessorProviderStorage     # Provider registry
  ├── StateStorage                 # State data (columns, rows, mappings)
  ├── ProcessorStorage             # Processor CRUD
  ├── ProcessorStateRouteStorage   # Route definitions (processor↔state)
  ├── UsageStorage                 # Usage reports by period
  ├── SessionStorage               # Session management
  ├── VaultStorage                 # Secrets
  ├── ConfigMapStorage             # Config maps
  ├── StateActionStorage           # State actions
  ├── FilterStorage                # Saved filters
  └── MonitorLogEventStorage       # Log events
```

`StateMachineStorage` uses a metaclass (`ForwardingStateMachineStorageMeta`) to delegate method calls to the appropriate sub-storage.

#### Key StateStorage Methods

| Method                              | Purpose                                      |
|-------------------------------------|----------------------------------------------|
| `load_state(state_id, offset, limit)` | Load state with optional pagination         |
| `fetch_state(state_id)`             | Get state metadata only                      |
| `insert_state(state)`               | Create new state                             |
| `update_state(state)`               | Update existing state                        |
| `append_state_data_direct(state_id, data)` | Incremental append (memory-efficient)  |
| `fetch_state_data_chunk_for_export(state_id, offset, limit)` | Streaming export |

---

### 6. Compiler Layer

Provides sandboxed Python execution using RestrictedPython.

```
BaseRunnable.instantiate(code)
  │
  ├── RestrictedPython.compile_restricted()
  ├── Set up restricted globals:
  │     ├── safe builtins (no exec, eval, import)
  │     ├── write guards (_write_, _getattr_, _getitem_)
  │     ├── allowed: requests, json, lists, dicts
  │     └── math, hashlib, datetime
  │
  ├── Execute compiled code
  └── Return instance of user's "Runnable" class
```

The `safer_evaluate()` function in `utils/evaluate.py` provides expression-level sandboxing:
- Allowed: `sum`, `range`, `math`, `random`, `hashlib`, `hashit`, `now`, `rand_hash`
- All attribute/item/iterator access is guarded

---

### 7. Filter System

28 operators for runtime data filtering:

```
Comparison:  EQ, NE, GT, GTE, LT, LTE
Collection:  IN, NOT_IN
String:      CONTAINS, NOT_CONTAINS, STARTS_WITH, ENDS_WITH
Regex:       REGEX, REGEX_CASE_INSENSITIVE
Existence:   EXISTS, NOT_EXISTS, IS_NULL, IS_NOT_NULL
Range:       BETWEEN, NOT_BETWEEN
```

Filters are applied as AND logic across all conditions. Dot-notation supported for nested keys (e.g., `context.run_id`).

---

## Flags Reference

### BaseStateConfig Flags

| Flag                                  | Default | Description                                      |
|---------------------------------------|---------|--------------------------------------------------|
| `flag_append_to_session`              | False   | Append entries to session context                |
| `flag_dedup_drop_enabled`             | False   | Drop duplicates by input hash                   |
| `flag_enable_execute_set`             | False   | Batch processing mode (all entries at once)      |
| `flag_enable_execute_set_inherit_set` | False   | Inherit full set in batch mode                   |
| `flag_flatten_on_save`                | True    | Flatten nested dicts to dot-notation             |
| `flag_keep_raw_output`                | True    | Store raw processor output                       |
| `flag_include_provider_info`          | True    | Include provider metadata in output              |
| `flag_include_processing_created_at`  | True    | Track creation timestamp per entry               |

### StateConfig Flags

| Flag                                      | Default | Description                                  |
|-------------------------------------------|---------|----------------------------------------------|
| `flag_require_primary_key`                | False   | Enforce primary key presence                 |
| `flag_query_state_inheritance_all`        | True    | Inherit all input columns to output          |
| `flag_query_state_inheritance_inverse`    | False   | Reverse inheritance direction                |
| `flag_auto_save_output_state`             | False   | Auto-persist output state                    |
| `flag_auto_route_output_state`            | False   | Auto-route output downstream                 |
| `flag_auto_route_output_state_after_save` | False   | Route only after save completes              |
| `flag_expect_stream`                      | False   | Enable streaming mode                        |

### StateConfigLM Flags

| Flag                          | Default | Description                              |
|-------------------------------|---------|------------------------------------------|
| `flag_include_prompts_in_state` | False | Include rendered prompts as state columns |

---

## Processor Properties

### ProcessorPropertiesBase

| Property               | Type   | Default    | Description                           |
|------------------------|--------|------------|---------------------------------------|
| `requestDelay`         | int    | 0          | Delay between requests (ms)           |
| `maxBatchSize`         | int    | 100        | Max rows per batch                    |
| `maxBatchLimit`        | int    | 1          | Max entries per NATS message          |
| `concurrencyMode`      | enum   | PROJECT_ID | Partition key strategy                |
| `concurrencyExpression` | str   | None       | Custom expression for EXPRESSION mode |

### ProcessorPropertiesLM (extends Base)

| Property            | Type  | Default | Description                   |
|---------------------|-------|---------|-------------------------------|
| `topK`              | int   | None    | Top-K sampling                |
| `topP`              | float | None    | Nucleus sampling              |
| `maxTokens`         | int   | None    | Max output tokens             |
| `temperature`       | float | None    | Sampling temperature          |
| `repeatPenalty`      | float | None    | Repetition penalty            |
| `presencePenalty`    | float | None    | Presence penalty              |
| `frequencyPenalty`   | float | None    | Frequency penalty             |
| `override_base_url` | str   | None    | Custom API endpoint           |

---

## Key Design Decisions

1. **Storage is interface-only**: Core defines contracts, `alethic-ism-db` provides Postgres implementations. This keeps core dependency-free from database drivers.

2. **Polymorphic state configs**: One `State` class supports many config types (LM, DB, Code, Visual, Audio, Stream) through inheritance. The config type determines processor behavior.

3. **NATS for all messaging**: Every inter-service communication uses NATS JetStream. Messages are JSON with `type`, `route_id`/`processor_id`, and `query_state` payload.

4. **Partitioned routing**: Messages route to `{subject}.{state_id}` subjects for parallelism. Concurrency mode determines the partition key.

5. **RestrictedPython sandbox**: User-provided code (processor-python, edge functions) executes in a RestrictedPython sandbox with guarded attribute/item access.

6. **Dedup by input hash**: When `flag_dedup_drop_enabled=True`, entries are SHA256-hashed by primary key. Duplicates are silently dropped.

7. **State inheritance**: Output states can inherit columns from input states. Controlled by `query_state_inheritance` (selective) or `flag_query_state_inheritance_all` (full).
