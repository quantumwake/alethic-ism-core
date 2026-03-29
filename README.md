# Alethic ISM Core — Python SDK

The core Python SDK for the Alethic Instruction-Based State Machine (ISM) framework. Provides models, messaging, processing, storage interfaces, and utilities shared across all Python-based ISM services.

## What is ISM?

ISM is a distributed state machine framework for orchestrating instruction-based data processing pipelines. A **processor** receives input state, executes logic (LLM calls, code, transforms), and produces output state that routes to downstream processors via NATS messaging.

```
Input State ──► Processor ──► Output State ──► Next Processor ──► ...
                   │                               │
                   ▼                               ▼
              NATS Message                    NATS Message
```

This repository (`alethic-ism-core`) is the shared foundation. It defines the contracts — models, interfaces, base classes — that every other ISM service depends on.

## Ecosystem Position

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         alethic-ism-core (this repo)                   │
│  Models │ Messaging │ Processors │ Storage Interfaces │ Utils │ Vault  │
└────┬────────┬───────────┬──────────────┬──────────────┬───────────┬────┘
     │        │           │              │              │           │
     ▼        ▼           ▼              ▼              ▼           ▼
  ism-db   state-     processor-*    state-sync-    ism-api     ism-monitor
 (Postgres  router    (openai,       store          (FastAPI)
  impl)   (dispatch)  anthropic,    (persistence)
                      gemini, ...)
```

| Dependent Service              | What it uses from core                                          |
|---------------------------------|-----------------------------------------------------------------|
| `alethic-ism-db`                | All storage interfaces, all models, BaseProcessor, embeddings   |
| `alethic-ism-state-router`      | BaseMessageConsumer, Router, NATSMessageProvider, models        |
| `alethic-ism-processor-openai`  | BaseProcessorLM, BaseMessageConsumerProcessor, MonitoredUsage   |
| `alethic-ism-processor-anthropic` | BaseProcessorLM, BaseMessageConsumerProcessor, MonitoredUsage |
| `alethic-ism-processor-gemini`  | BaseProcessorLM, BaseMessageConsumerProcessor, MonitoredUsage   |
| `alethic-ism-processor-python`  | BaseProcessor, SecureRunnable, BaseMessageConsumerProcessor     |
| `alethic-ism-state-sync-store`  | NATSRouteBatch, all storage interfaces, all models              |
| `alethic-ism-monitor`           | BaseMessageConsumer, MonitoredProcessorState, MonitorLogEvent   |

A Go equivalent exists at `alethic-ism-core-go` with parallel packages (`pkg/routing/nats`, `pkg/repository/*`, `pkg/data/models`, `pkg/cache`, `pkg/auth`, `pkg/s3`, `pkg/crypto`). The Go version is consumed by `alethic-ism-file-source`, `alethic-ism-state-sync-s3`, and other Go services.

## Package Structure

```
src/ismcore/
├── model/                  # Data models, enums, state definitions
│   ├── base_model.py       # Processor, ProcessorState, ProcessorProvider, enums
│   ├── processor_state.py  # State, StateConfig*, columns, row data, keys
│   ├── filter.py           # FilterOperator (28 ops), Filter, FilterItem
│   └── base_model_usage_and_limits.py  # Usage tracking, tier limits
│
├── messaging/              # NATS-based message routing
│   ├── base_message_route_model.py     # BaseRoute abstract class
│   ├── base_message_provider.py        # BaseMessageConsumer, BaseRouteProvider
│   ├── base_message_consumer_processor.py  # Processor-specific consumer
│   ├── base_message_router.py          # Router (route registry)
│   ├── nats_message_provider.py        # NATSMessageProvider factory
│   ├── nats_message_route.py           # NATSRoute (JetStream pull consumer)
│   ├── nats_message_route_concurrent.py # NATSRouteConcurrent (parallel workers)
│   ├── nats_message_route_batch.py     # NATSRouteBatch (batch fetch + group)
│   └── errors.py                       # RouteNotFoundError
│
├── processor/              # Processor base classes
│   ├── base_processor.py              # BaseProcessor, StatePropagation*
│   ├── base_processor_lm.py           # BaseProcessorLM (language models)
│   ├── base_processor_visual.py       # BaseProcessorVisual (image models)
│   ├── monitored_processor_state.py   # Monitoring + usage tracking
│   └── processor_full_join.py         # Full join processor
│
├── storage/                # Storage layer (abstract interfaces)
│   ├── processor_state_storage.py     # All storage interfaces + StateMachineStorage
│   ├── consumer_route_storage.py      # Consumer route storage
│   └── redis_storage.py               # Redis session implementation
│
├── compiler/               # Safe code execution (RestrictedPython)
│   ├── runnable.py                    # BaseRunnable, dynamic code compilation
│   └── secure_runnable.py             # BaseSecureRunnable, sandbox guards
│
├── embedding/              # Embedding + similarity
│   ├── embedding_utils.py             # create_embedding, calculate_embeddings
│   ├── semantic_distance.py           # Semantic similarity scoring
│   ├── syntactic_accuracy.py          # Syntactic accuracy scoring
│   └── datasource.py                  # Embedding data sources
│
├── utils/                  # Shared utilities
│   ├── general_utils.py               # YAML, hashing, templates, stopwatch
│   ├── evaluate.py                    # safer_evaluate (RestrictedPython)
│   ├── state_utils.py                 # Processor status transition validators
│   ├── map_utils.py                   # flatten() for nested dicts/lists
│   ├── ism_logger.py                  # Logger factory (LOG_LEVEL from env)
│   └── old_state_utils.py             # Deprecated
│
└── vault/                  # Secrets management models
    └── vault_model.py                 # Vault, ConfigMap, VaultType, ConfigMapType
```

## Core Concepts

### State

A `State` is the fundamental data container. It holds columnar data (like a table) with metadata about how that data flows through the system.

```python
State(
    id="state-123",
    project_id="project-456",
    config=StateConfig(...),       # How this state behaves
    columns={"col_a": StateDataColumnDefinition(...)},
    data={"col_a": StateDataRowColumnData(values=[...], count=N)},
    mapping={"row_key": StateDataColumnIndex(indices=[0, 1])},
    count=N
)
```

### Processor

A `Processor` is a unit of execution. It reads from input state(s), runs logic, and writes to output state(s).

```
┌──────────────────────────────────────┐
│            Processor                 │
│  ┌──────────┐    ┌────────────────┐  │
│  │  Input    │───►│   execute()    │  │
│  │  State    │    │  (LLM call,   │  │
│  └──────────┘    │   code, etc.)  │  │
│                  └───────┬────────┘  │
│                          │           │
│                  ┌───────▼────────┐  │
│                  │  Output State  │  │
│                  └────────────────┘  │
└──────────────────────────────────────┘
```

### ProcessorState (Route)

A `ProcessorState` connects a processor to a state with a direction (INPUT or OUTPUT). This is the "route" — it defines the data flow edges in the processing graph.

### Message Flow

```
┌─────────┐     ┌──────────────┐     ┌───────────┐     ┌──────────────┐
│  API /   │────►│ State Router │────►│ Processor │────►│ State Router │──► ...
│ Trigger  │     │ (dispatch)   │     │ Consumer  │     │ (next hop)   │
└─────────┘     └──────────────┘     └───────────┘     └──────────────┘
                  NATS subject         NATS subject       NATS subject
```

Message types:
- `query_state_entry` — State-triggered: upstream completed, route downstream
- `query_processor_entry` — Direct trigger: API sends to specific processor
- `query_state_route` — Batch: re-execute all rows through a route
- `query_state` — Actual data payload delivered to processor consumers

## Quick Start

### Installation

```bash
pip install alethic-ism-core
```

Or from source:

```bash
git clone https://github.com/quantumwake/alethic-ism-core.git
cd alethic-ism-core
uv venv && source .venv/bin/activate
uv pip install -e .
```

### Dependencies

```
requests, restrictedpython, pydantic, pyyaml, nats-py, mako, python-dotenv
```

### Python >= 3.10

## Configuration

### NATS Route Configuration (`routing-nats.yaml`)

```yaml
messageConfig:
  routes:
    - name: MY_PROCESSOR
      url: nats://localhost:4222
      subject: processor.my.subject
      selector: my/processor/selector
      queue: my_queue
      ack_wait: 90
      batch_size: 1
      jetstream_enabled: true
      concurrent_enabled: false
      concurrent_max_workers: 10
```

### Environment Variables

| Variable            | Default | Description                   |
|---------------------|---------|-------------------------------|
| `LOG_LEVEL`         | DEBUG   | Python logging level          |
| `FLAG_CONSUMER_WAIT`| —       | Consumer blocking flag        |

## Releasing

```bash
export ISM_CORE_VERSION=v1.0.x
git tag -a ${ISM_CORE_VERSION} -m "Release version ${ISM_CORE_VERSION}"
git push origin ${ISM_CORE_VERSION}
```

Versioning is managed by `setuptools_scm` from git tags.

## License

Dual-licensed:

- **AGPL v3** — Academic, research, and nonprofit use. Derivative works must remain open source.
- **Commercial License** — Production and proprietary use. Contact for details.

Authors: Kasra Rasaee, Sankalpa Ghose
