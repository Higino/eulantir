# Eulantir

A generic data pipeline orchestrator where the pipeline code is written by an LLM agent.

You describe what you want in plain English. Eulantir generates the pipeline config and transform logic, then runs it.

## How it works

```
You: "read CSV from S3, remove rows where status is null, write to Postgres"
         ↓
   LLM Agent (Ollama / OpenAI / Claude)
         ↓
   pipeline.yaml  +  generated/filter_status.go
         ↓
   eulantir validate pipeline.yaml
         ↓
   eulantir run pipeline.yaml
         ↓
   ✔  read    in:12430  out:12430  dlq:0   42ms
   ✔  filter  in:12430  out:11891  dlq:0   18ms
   ✔  write   in:11891  out:11891  dlq:0  104ms
```

The LLM runs **only at generation time**. Once the files are written, the pipeline is pure Go — no LLM calls during data processing. Generated transform files are compiled into native Go plugins (`.so`) on first run and cached for subsequent runs.

---

## Quick start

**Requirements:** Go 1.21+, Ollama

```bash
# 1. Install and start Ollama
brew install ollama
brew services start ollama
ollama pull llama3.2

# 2. Build
git clone https://github.com/higino/eulantir
cd eulantir
go build -o eulantir .

# 3. Generate a pipeline from plain English
./eulantir generate \
  --output ./my-pipeline \
  "read a CSV file, filter rows where status is not active, write to Postgres"

# 4. Validate before running
./eulantir validate my-pipeline/pipeline.yaml

# 5. Run it
./eulantir run my-pipeline/pipeline.yaml
```

---

## Commands

| Command | Description | Status |
|---|---|---|
| `eulantir generate "<intent>"` | Generate pipeline.yaml + transform code from plain English | ✅ |
| `eulantir validate <pipeline.yaml>` | Validate config, refs, and DAG — no execution | ✅ |
| `eulantir run <pipeline.yaml>` | Execute a pipeline | ✅ |
| `eulantir connectors list` | List available connector types | ✅ |
| `eulantir connectors info <type>` | Show config keys for a connector | ✅ |

### Generate flags

```
--provider   LLM provider: ollama | openai | anthropic  (default: ollama)
--model      Model name                                  (default: llama3.2)
--output     Output directory for generated files        (default: .)
--base-url   Override LLM endpoint URL
--validate   Validate the pipeline immediately after generation
--run        Validate and run the pipeline immediately after generation
-v           Verbose logging (shows token counts, retry attempts)
```

---

## What is a config?

A config (`pipeline.yaml`) describes a pipeline. It answers four questions:

1. **What systems are connected?** → `connectors`
2. **What logic transforms the data?** → `transforms`
3. **In what order do things run?** → `tasks` + `depends_on`
4. **How are failures handled?** → `retry` + `dlq`

```yaml
version: "1"
name: filter-and-load

connectors:
  - name: source
    type: csv
    config:
      path: ./data/input.csv

  - name: destination
    type: postgres
    config:
      dsn: $POSTGRES_DSN        # $VAR values read from environment at load time
      table: public.clean_data
      upsert_key: [id]          # makes writes idempotent (safe to retry)

transforms:
  - name: filter-active
    source: generated/filter_active.go   # LLM-generated
    entrypoint: FilterActive             # exported Go function

tasks:
  - id: read
    connector: source
    depends_on: []

  - id: filter
    transform: filter-active    # transform-only: no connector, receives from upstream
    depends_on: [read]

  - id: write
    connector: destination
    depends_on: [filter]

retry:
  max_attempts: 3
  initial_interval: 1s
  multiplier: 2.0
  max_interval: 30s
  jitter: true

dlq:
  enabled: true
  type: file
  config:
    path: ./dlq
```

### Config requirements

Every field validated before the pipeline runs:

| Field | Rule |
|---|---|
| `version` | Must be `"1"` |
| `name` | Required |
| `connectors` | At least one; names must be unique |
| `connectors[].type` | Must match a registered type (`eulantir connectors list`) |
| `tasks` | At least one; IDs must be unique |
| `tasks[].connector` or `.transform` | Must have at least one |
| Every `connector`/`transform` ref in a task | Must exist in the lists above |
| Every ID in `depends_on` | Must be an existing task ID |
| The full task graph | Must have no cycles |

---

## What is a DAG?

**DAG** = Directed Acyclic Graph. Every task is a node; every `depends_on` entry is a directed edge pointing from dependency to dependent.

```
tasks:                              DAG in memory:
  read    depends_on: []
  filter  depends_on: [read]        read ──▶ filter ──▶ write
  write   depends_on: [filter]
```

The DAG determines two things:

- **Order** — a task only starts after all its dependencies finish
- **Parallelism** — tasks with no dependency between them run at the same time

Fan-out and fan-in are valid:

```
            ┌──▶ filter-nulls ──┐
read ───────┤                   ├──▶ write
            └──▶ filter-dupes ──┘
```

Here `filter-nulls` and `filter-dupes` run concurrently; `write` waits for both.

A **cycle** (`a → b → c → a`) is caught by `eulantir validate` before anything runs:

```
Error: DAG error: pipeline DAG contains a cycle — check depends_on fields
```

---

## How data flows between tasks

Tasks connect via **buffered Go channels**. Each task gets an output channel; its downstream task reads from it. Source and sink run as concurrent goroutines:

```
source task                           sink task
┌──────────────────┐  chan []Record  ┌──────────────────┐
│ src.ReadBatch()  │ ───────────────▶│ sink.WriteBatch() │
└──────────────────┘                 └──────────────────┘
```

When the source exhausts its input it closes the channel — the sink's loop terminates naturally. No data is buffered in memory beyond the channel's 10-batch buffer.

---

## Available connectors

```
$ eulantir connectors list

TYPE          DESCRIPTION
------------  ----------------------------------------
csv           Read records from a local CSV file
              config: path, delimiter, has_header
csv-sink      Write records to a local CSV file
              config: path
postgres      Write records to a PostgreSQL table (upsert)
              config: dsn, table, upsert_key
```

---

## Examples

### Generate → validate → run (three separate commands)

```bash
$ eulantir generate --output ./orders \
    "read customer orders CSV, filter rows where total_amount is zero, write to Postgres"

🤖 Generating pipeline...
   Provider : ollama   Model : llama3.2
   Intent   : read customer orders CSV, filter rows where total_amount is zero, write to Postgres

✔  Written: ./orders/pipeline.yaml
✔  Written: ./orders/generated/filter_orders.go

✅ Done. Next steps:
   eulantir validate ./orders/pipeline.yaml
   eulantir run      ./orders/pipeline.yaml

$ eulantir validate ./orders/pipeline.yaml

Validating ./orders/pipeline.yaml
  ✔  schema valid
  ✔  2 connector(s), 1 transform(s), 3 task(s)
  ✔  DAG is acyclic

Execution order:
  1. read    [source]
  2. filter  [(transform only) + transform:filter-orders]
  3. write   [destination]

✅  orders-pipeline is valid

$ eulantir run ./orders/pipeline.yaml

▶  Running pipeline "orders-pipeline" (3 tasks)
  ✔  read    in:5000   out:5000   dlq:0   12ms
  ✔  filter  in:5000   out:4821   dlq:0   820ms   ← first run: plugin compiled
  ✔  write   in:4821   out:4821   dlq:0   91ms
✅  Pipeline "orders-pipeline" finished successfully
```

### Generate → validate → run (one command with `--run`)

```bash
$ eulantir generate --output ./orders --run \
    "read customer orders CSV, filter rows where total_amount is zero, write to Postgres"

🤖 Generating pipeline...
✔  Written: ./orders/pipeline.yaml
✔  Written: ./orders/generated/filter_orders.go

Validating ./orders/pipeline.yaml
  ✔  schema valid
  ✔  DAG is acyclic
✅  orders-pipeline is valid

▶  Running pipeline "orders-pipeline" (3 tasks)
  ✔  read    in:5000   out:5000   dlq:0   12ms
  ✔  filter  in:5000   out:4821   dlq:0   820ms
  ✔  write   in:4821   out:4821   dlq:0   91ms
✅  Pipeline "orders-pipeline" finished successfully
```

### Run a pipeline with a transform node

Filter a CSV — keep only active customers, write result to a new CSV:

```
$ eulantir run testdata/pipelines/transform_pipeline.yaml

▶  Running pipeline "csv-filter-transform-test" (3 tasks)

  ✔  read    in:5   out:5   dlq:0   0s
  ✔  filter  in:5   out:3   dlq:0   714ms   ← plugin compiled + loaded on first run
  ✔  write   in:3   out:3   dlq:0   0s

✅  Pipeline "csv-filter-transform-test" finished successfully
```

Input (`customers.csv`, 5 rows) → output (3 rows — Bob and Dave dropped, status ≠ active):

```
id,name,status
1,Alice,active
3,Carol,active
5,Eve,active
```

The 714ms on the `filter` node is the **one-time plugin compilation**. On subsequent runs with the same transform source, the cached `.so` is loaded in milliseconds.

---

### Validate catches every structural error

```
# Cycle in task graph
Error: DAG error: pipeline DAG contains a cycle — check depends_on fields

# Task references unknown connector
Error: config error: task "read" references unknown connector "does-not-exist"

# depends_on points to unknown task
Error: config error: task "read" depends_on unknown task "ghost-task"

# Two tasks share an id
Error: config error: duplicate task id "read"

# Missing required field
Error: config error: validation errors:
  - field "Version" is required
```

### $ENV substitution

Connector config values starting with `$` are resolved from the environment at load time — secrets never live in the YAML file:

```yaml
connectors:
  - name: db
    type: postgres
    config:
      dsn: $POSTGRES_DSN
      table: $TARGET_TABLE
```

```bash
POSTGRES_DSN="postgres://user:pass@localhost/mydb" \
TARGET_TABLE="public.orders" \
eulantir run pipeline.yaml
```

---

## Generated transforms

When the LLM generates a transform, it produces a `.go` file. At pipeline run time, Eulantir compiles it into a native Go **plugin** (`.so` shared library) and loads it dynamically — no interpreter, full compiled speed, any imports allowed.

```
generated/filter_active.go
         ↓
go build -buildmode=plugin -o .eulantir-cache/filter_active.so generated/filter_active.go
         ↓
plugin.Open(".eulantir-cache/filter_active.so")
         ↓
plugin.Lookup("FilterActive")  →  called per record batch
```

Compiled plugins are **cached by content hash** — the build only runs once per unique source file.

### Transform file format

```go
package main   // required — Go plugin build mode demands package main

import (
    "context"
    "encoding/json"
    // any stdlib or third-party imports are fine
)

type Record struct {
    Key     []byte
    Value   []byte            // JSON row — parse with encoding/json
    Headers map[string]string
    Offset  int64
}

// Return nil, nil        → drop record (filter)
// Return []Record{in}   → pass through or modify (map)
// Return []Record{a, b} → split into multiple records (fanout)
func FilterActive(ctx context.Context, in Record) ([]Record, error) {
    var row map[string]any
    json.Unmarshal(in.Value, &row)
    if row["status"] != "active" {
        return nil, nil   // drop
    }
    return []Record{in}, nil
}
```

> **Platform note**: Go plugins work on Linux and macOS only. Windows is not supported.

---

## LLM providers

Eulantir is provider-agnostic. Ollama and OpenAI share one adapter (both speak `/v1/chat/completions`).

| Provider | Flags | Credential |
|---|---|---|
| **Ollama** (local) | `--provider ollama --model llama3.2` | none |
| **OpenAI** | `--provider openai --model gpt-4o` | `OPENAI_API_KEY` |
| **Anthropic** | `--provider anthropic --model claude-opus-4-5` | `ANTHROPIC_API_KEY` *(Phase 7)* |

---

## Architecture

```
cmd/                   CLI (cobra) — no business logic
internal/
  agent/               LLM agent: prompt building, response parsing, retry loop
  llm/                 Provider interface + adapters (Ollama/OpenAI share one)
  config/              Pipeline YAML schema, loader, $ENV substitution, validation
  dag/                 DAG + Kahn's topological sort + cycle detection
  engine/              Pipe-based concurrent task executor + retry + DLQ wiring
  connector/           Record, Source, Sink interfaces + Default registry
  connectors/csv/      CSV source and sink
  connectors/postgres/ Postgres sink (pgx batch upsert)
  transform/           Transform interface + Go plugin loader  [Phase 4]
  dlq/                 FileDLQ: NDJSON per node per day
  lineage/             OpenLineage event emitter            [Phase 6]
testdata/pipelines/    valid.yaml, cycle.yaml
```

Key decisions:
- **Pipe-based execution** — source and sink goroutines run concurrently, connected by buffered channels
- **At-least-once + idempotent sinks** — Postgres uses `ON CONFLICT DO UPDATE SET`
- **Go plugin for transforms** — generated Go compiled to `.so` via `go build -buildmode=plugin`, loaded with `plugin.Open()`; full language + any imports; Linux/macOS only
- **Single binary** — no external scheduler, queue, or database required

---

## Status

| Phase | Feature | Status |
|---|---|---|
| 0 | CLI skeleton + all interfaces | ✅ |
| 1 | LLM agent (`generate` command) | ✅ |
| 2 | Config loader + DAG validator (`validate` command) | ✅ |
| 3 | Pipeline engine + CSV/Postgres + `run` command | ✅ |
| 4 | Transform loading via Go plugin | ✅ |
| 5 | Full generate → validate → run loop (MVP) | ✅ |
| 6 | OpenLineage lineage + structured logging | Pending |
| 7 | S3, Kafka, Anthropic adapter | Pending |
