# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this project is

Eulantir is a generic data pipeline orchestrator written in Go. It has two jobs:

1. **Generate** — takes a plain-English intent, calls an LLM, and writes a `pipeline.yaml` config + Go transform source files to disk.
2. **Run** — parses the pipeline YAML, builds a DAG, compiles generated transform code into a Go plugin (`.so`) at runtime, loads it with `plugin.Open()`, and executes tasks in dependency order with retries and a dead-letter queue.

The LLM only runs at generation time. Once files are on disk, pipeline execution is pure Go.

## Commands

```bash
# development
go build ./...                              # build all packages
go run main.go <command>                    # run without installing
go test ./...                               # run all tests
go test ./internal/dag/... -v               # run a single package's tests
go test ./internal/config/... -run TestLoad # run a single test

# CLI
go run main.go generate "<intent>"
go run main.go generate \
  --provider ollama --model llama3.2 \
  --output ./my-pipeline "<intent>"

go run main.go validate pipeline.yaml       # validate config + DAG, print execution order
go run main.go run pipeline.yaml            # execute a pipeline
go run main.go connectors list              # list registered connector types
go run main.go connectors info csv          # show config keys for a connector type
go run main.go visualize pipeline.yaml      # export interactive DAG as self-contained HTML
go run main.go serve pipeline.yaml          # live dashboard (view only)
go run main.go serve pipeline.yaml --run    # live dashboard + execute pipeline
```

## Architecture

### Request flow: `generate`

```
intent string
  → agent.Agent.Generate()
      → agent.SystemPrompt()           builds system prompt with YAML schema + connector catalog
      → llm.Provider.Complete()        calls Ollama / OpenAI / Anthropic
      → agent.Parse()                  extracts ```yaml and ```go fenced blocks via regex
      → retry loop (max 3×)            feeds parse errors back to LLM as correction messages
  → writes pipeline.yaml + generated/*.go to output directory
```

### Request flow: `validate`

```
pipeline.yaml
  → config.Load()
      → os.ReadFile + os.ExpandEnv      reads file, substitutes $VAR references
      → yaml.Unmarshal                  parses into PipelineConfig struct
      → validator.Struct()              checks required fields and minimums
      → validateConfig()                cross-validates names, refs, depends_on
  → dag.Build(cfg.Tasks)
      → dag.AddNode() × N               one node per TaskConfig
      → dag.AddEdge() × M               one edge per depends_on entry
      → dag.TopologicalSort()           Kahn's algorithm — errors on cycle
  → prints execution order to stderr
```

### Request flow: `run`

```
pipeline.yaml
  → config.Load()                       same validation as validate
  → dag.Build()                         produces sorted []Node execution order
  → engine.LocalEngine.Run()
      → creates one buffered chan Batch per task (the "pipes")
      → launches every task as a goroutine simultaneously
      → source task:  ReadBatch() → pipe → ...
      → sink   task:  ... → pipe → WriteBatch()
      → on read/write error: exponential backoff retry (cenkalti/backoff)
      → on retry exhaustion: push records to FileDLQ (.jsonl)
      → emits TaskResult{RecordsIn, RecordsOut, RecordsDLQ, Duration} per task
  → transform loading via Go plugin (Phase 4):
      → exec.Command("go build -buildmode=plugin") compiles generated .go → .so
      → plugin.Open() loads the .so
      → plugin.Lookup(entrypoint) resolves the exported function
      → called per record between source read and sink write
  → lineage events (Phase 6):
      → lineage.New(cfg.Lineage)       builds HTTPEmitter or NoopEmitter from config
      → per task: EmitStart before run, EmitComplete or EmitFail after run
      → posts OpenLineage JSON to endpoint/api/v1/lineage (e.g. Marquez)
      → pipeline start/finish logged via slog
```

### Request flow: `visualize`

```
pipeline.yaml
  → config.Load()                       same validation as validate
  → visualize.Build(cfg)                builds GraphData{Nodes, Edges} from tasks + connector types
      → node kind inferred from connector type (source / sink / transform)
      → all nodes initialised with StatusPending
  → visualize.RenderHTML(graph, liveMode=false)
      → json.Marshal(GraphData) → embedded JS constant
      → html/template executes htmlTmpl → self-contained .html (vis-network via CDN)
  → os.WriteFile → <pipeline-name>.html
```

### Request flow: `serve`

```
pipeline.yaml
  → config.Load()                       same validation as validate
  → visualize.Build(cfg)                produces initial GraphData (all pending)
  → visualize.NewServer(graph)          creates SSE broadcast server
  → http.Server{Handler: srv}.ListenAndServe(:8080)
      GET /         → RenderHTML(graph, liveMode=true)  (SSE client JS injected)
      GET /events   → text/event-stream; one JSON frame per TaskResult
  → (optional --run flag)
      → engine.LocalEngine.Run(ctx, cfg)  executes pipeline in background goroutine
      → for each TaskResult: srv.Push(result)
          → graph.ApplyResult()           updates in-memory node status
          → broadcasts {"nodeID":"…","status":"…"} to all SSE clients
      → srv.Done()                        sends sentinel frame; browser closes stream
  → blocks until SIGINT / SIGTERM → httpSrv.Shutdown()
```

### Package map

| Package | Responsibility |
|---|---|
| `cmd/` | Cobra command definitions only — no business logic |
| `internal/llm` | `Provider` interface + `OpenAIAdapter` (Ollama + OpenAI) + `BuildProvider` factory |
| `internal/agent` | `Agent.Generate()`, `SystemPrompt()`, `Parse()` |
| `internal/config` | `PipelineConfig` structs, `Load()`, `LoadBytes()`, `$ENV` substitution, full cross-validation |
| `internal/dag` | `DAG`, `AddNode/AddEdge`, `TopologicalSort()` (Kahn's), `Build(tasks)` |
| `internal/engine` | `LocalEngine`, pipe-based task execution, `TaskResult`, retry wiring |
| `internal/connector` | `Record`, `Source`, `Sink` interfaces + `Registry` (Default singleton) |
| `internal/connectors/csv` | `Source` (reads CSV → JSON records) + `Sink` (writes JSON records → CSV) |
| `internal/connectors/postgres` | `Sink` (upserts via pgx batch + ON CONFLICT DO UPDATE) |
| `internal/transform` | `Transform` interface + Go plugin loader (`Load(source, entrypoint)`) — Phase 4 |
| `internal/dlq` | `DLQ` interface + `FileDLQ` (NDJSON per node per day) |
| `internal/lineage` | `Emitter` interface + `RunEvent`, `NoopEmitter`, `HTTPEmitter`, `New()` factory — Phase 6 |
| `internal/codegen` | `GeneratedPipeline` struct — Phase 5 |
| `internal/visualize` | `Build()` (config → graph), `RenderHTML()` (static export), `Server` (SSE live dashboard) |

## What is a config

A config (`pipeline.yaml`) is the human-readable description of a pipeline. It answers:

1. **What systems are connected?** → `connectors`
2. **What logic transforms the data?** → `transforms`
3. **In what order do things run?** → `tasks` + `depends_on`
4. **How are failures handled?** → `retry` + `dlq`

### Config requirements

| Field | Required | Rule |
|---|---|---|
| `version` | ✅ | Must be exactly `"1"` |
| `name` | ✅ | Any non-empty string |
| `connectors` | ✅ | At least one; names must be unique |
| `connectors[].name` | ✅ | Unique across all connectors |
| `connectors[].type` | ✅ | Must match a registered connector type |
| `transforms[].name` | ✅ if present | Unique across all transforms |
| `transforms[].source` | ✅ if present | Path to Go file |
| `transforms[].entrypoint` | ✅ if present | Exported function name |
| `tasks` | ✅ | At least one; IDs must be unique |
| `tasks[].id` | ✅ | Unique across all tasks |
| `tasks[].connector` or `.transform` | ✅ | Must have at least one |
| `tasks[].connector` | if set | Must match a connector name |
| `tasks[].transform` | if set | Must match a transform name |
| `tasks[].depends_on[]` | if set | Every entry must be an existing task ID |
| DAG (all tasks) | implicit | No cycles allowed |

Config values prefixed with `$` (e.g. `$POSTGRES_DSN`) are resolved from environment variables at load time via `os.ExpandEnv`. Unknown variables become empty strings.

## What is a DAG

DAG = **Directed Acyclic Graph**. Every task is a node; every `depends_on` entry is a directed edge.

```
tasks:                              DAG:
  read    depends_on: []
  filter  depends_on: [read]        read ──▶ filter ──▶ write
  write   depends_on: [filter]
```

The DAG determines:
- **Execution order** — a task only starts after all its dependencies complete
- **Parallelism** — tasks with no dependency between them run concurrently

Fan-out and fan-in are valid:

```
            ┌──▶ filter-nulls ──┐
read ───────┤                   ├──▶ write
            └──▶ filter-dupes ──┘
```

`filter-nulls` and `filter-dupes` run in parallel; `write` waits for both.

A cycle (`a → b → c → a`) is rejected at validation time — both tasks would wait forever for each other.

`dag.Build(tasks)` is the single entry point: adds nodes, adds edges, runs `TopologicalSort()`, returns `(*DAG, []Node, error)` where `[]Node` is the validated execution order.

## How data flows between tasks

Tasks are connected by **buffered Go channels** (one per task, buffer = 10 batches):

```
source task                          sink task
┌──────────────────┐   chan Batch   ┌──────────────────┐
│ src.ReadBatch()  │ ─────────────▶ │ sink.WriteBatch() │
│ sends to outPipe │                │ reads from inPipe │
└──────────────────┘                └──────────────────┘
```

A source task closes its output pipe when exhausted — the sink task's `range inPipe` loop then terminates naturally. Both tasks run as goroutines from the moment `Run()` is called, so reading and writing happen concurrently.

The pipe for the last task in a linear chain is created but never read — it is closed by the executor with no downstream consumer.

## Connector registry

`connector.Default` is a process-wide `*Registry`. Built-in connectors register themselves via `init()` in their packages. The `run` and `connectors` commands import connector packages with blank imports to trigger registration:

```go
import _ "github.com/higino/eulantir/internal/connectors/csv"
import _ "github.com/higino/eulantir/internal/connectors/postgres"
```

The registry determines whether a connector type is a source or sink. Current heuristic in `executor.resolveConnectors()`:
- `postgres`, `csv-sink`, `kafka-sink`, `s3-sink` → sink
- everything else (`csv`, `s3`, `kafka`) → source

## Registered connectors

| Type | Role | Required config | Optional config |
|---|---|---|---|
| `csv` | Source | `path` | — |
| `csv-sink` | Sink | `path` | — |
| `postgres` | Sink | `dsn`, `table` | `upsert_key` |
| `s3` | Source | `bucket`, `key` | `region`, `format` (`csv`\|`ndjson`, default `csv`), `access_key_id`, `secret_access_key`, `endpoint_url` |
| `s3-sink` | Sink | `bucket`, `key` | `region`, `format` (`csv`\|`ndjson`, default `ndjson`), `access_key_id`, `secret_access_key`, `endpoint_url` |
| `kafka` | Source | `brokers`, `topic`, `group_id` | — |
| `kafka-sink` | Sink | `brokers`, `topic` | — |

### S3 connector notes

- **`format`** — `ndjson` (newline-delimited JSON) is the default for `s3-sink` and the recommended format for data lake / Lakehouse pipelines (directly queryable by Athena, Spark, Snowflake external tables). `csv` is the default for the `s3` source for backwards compatibility.
- **`endpoint_url`** — only needed for local S3-compatible servers such as LocalStack or MinIO. Leave it empty when targeting real AWS; the SDK resolves the correct endpoint automatically. When set, `UsePathStyle = true` is applied automatically because LocalStack/MinIO require path-style URLs (`http://host/bucket/key`) whereas AWS defaults to virtual-hosted-style (`http://bucket.host/key`).
- **`s3-sink` upload** — records are buffered in memory and uploaded via a single `PutObject` call on `Close()`. If the buffer is empty (no records written), `Close()` is a no-op and no AWS call is made.
- **Credentials** — both connectors use the standard AWS credential chain (env vars → `~/.aws/credentials` → EC2 instance role). Explicit `access_key_id` / `secret_access_key` override the chain when provided.

### Data lake pipeline example

```yaml
connectors:
  - name: raw
    type: s3
    config:
      bucket: raw-data
      key: events/2026-04-18.csv
  - name: lake
    type: s3-sink
    config:
      bucket: data-lake
      key: events/2026-04-18.ndjson   # NDJSON — queryable by Athena/Spark/Snowflake
tasks:
  - id: read
    connector: raw
  - id: write
    connector: lake
    depends_on: [read]
```

## LLM provider abstraction

Ollama and OpenAI share one adapter (`OpenAIAdapter`) — both speak `/v1/chat/completions`. Only `base_url` and `api_key` differ.

```go
llm.NewOllamaAdapter("")       // http://localhost:11434
llm.NewOpenAIAdapter(apiKey)   // https://api.openai.com
llm.BuildProvider(cfg)         // factory from ProviderConfig
```

Anthropic adapter planned for Phase 7.

## Generate flags

```
--provider   ollama | openai | anthropic   (default: ollama)
--model      model name                     (default: llama3.2)
--output     output directory               (default: .)
--base-url   override LLM endpoint URL
--validate   validate the pipeline immediately after generation
--run        validate + run the pipeline immediately after generation
-v           verbose (token counts, retries)
```

## Generated transform contract

Generated transforms are compiled into Go plugins at runtime. The LLM must follow this exact template:

```go
package main   // ← MUST be "main" — required by Go plugin build mode

import (
    "context"
    "encoding/json"
    // any stdlib or third-party imports are allowed
)

// Record mirrors connector.Record — the engine passes values as JSON in Value.
type Record struct {
    Key     []byte
    Value   []byte            // JSON-encoded row — use encoding/json to parse
    Headers map[string]string
    Offset  int64
}

// Filter:  return nil, nil        (drop the record)
// Map:     return []Record{in}, nil  (pass through or modify)
// Fanout:  return []Record{a, b}, nil
func MyTransform(ctx context.Context, in Record) ([]Record, error) {}
```

Key rules enforced by `agent.SystemPrompt()` and validated by `agent.Parse()`:
- First line **must** be `package main` — Go plugin build mode requires it
- `entrypoint` in the YAML `transforms` block must match the exported function name exactly
- Third-party imports are allowed (unlike yaegi); the `go` toolchain is present at runtime
- `agent.Parse()` accepts `// transform: name` or `// transform/name.go` as the first-line header after `package main`
- Compiled `.so` files are cached by source content hash to avoid recompiling on every run

**Platform note**: Go plugins work on **Linux and macOS only**. Windows is not supported. This is an accepted constraint for Eulantir — target environments are Linux/macOS servers.

## DLQ

`FileDLQ` writes failed records as NDJSON under `<baseDir>/<nodeID>/<YYYY-MM-DD>.jsonl`. Each line is a `dlqEntry` with offset, key, value, headers, reason, and timestamp. `Drain()` returns a channel for replaying failed records.

## Lineage

Eulantir emits [OpenLineage](https://openlineage.io)-compatible events at the start and end of every task execution. This creates an auditable, queryable record of every data movement — which task read from where, how many records flowed through, and whether it succeeded or failed.

### What lineage is for

- **Data debugging** — trace wrong numbers back through every transform that touched the data
- **Run history** — structured record of past pipeline executions with record counts per task
- **Dependency mapping** — which pipelines consume which datasets
- **Compliance** — auditable proof of what data moved where and when

### OpenLineage and Marquez

**OpenLineage** is the wire protocol — a CNCF open standard (Apache 2.0) for lineage events. It is a specification, not a product. Eulantir implements it from scratch using only `net/http` and `encoding/json`. There is no SDK dependency.

**Marquez** is the recommended self-hosted backend (Apache 2.0, free, no paid tier):

```bash
# start Marquez + its Postgres backend
docker run -p 5000:5000 -p 5001:5001 marquezproject/marquez

# API:  http://localhost:5000
# UI:   http://localhost:5001
```

Because Eulantir speaks the standard OpenLineage format, any compatible backend works — Marquez, DataHub, Atlan, Apache Atlas — with nothing more than a one-line config change.

### Enabling lineage in pipeline.yaml

```yaml
lineage:
  enabled: true
  endpoint: http://localhost:5000   # Marquez (or any OpenLineage-compatible backend)
  namespace: my-org                 # groups pipelines together in the backend UI
```

When `enabled: false` or `endpoint` is empty, a `NoopEmitter` is used — zero overhead, zero HTTP calls.

### What gets emitted

For each task, three events are posted to `endpoint/api/v1/lineage`:

```
EmitStart    → { "eventType": "START",    "runId": "uuid", "jobName": "pipeline.task" }
EmitComplete → { "eventType": "COMPLETE", "jobFacets": { "recordsIn": N, "recordsOut": M, "recordsDLQ": K } }
EmitFail     → { "eventType": "FAIL",     "error": "..." }
```

### Architecture fit

Lineage is **orthogonal** to the pipeline execution — the engine runs identically whether lineage is wired up or not. `NoopEmitter` means zero impact on pipelines that don't configure it. Nothing in the executor or connectors knows about lineage.

### Verifying events reached Marquez

```bash
# list all jobs Marquez knows about
curl http://localhost:5000/api/v1/namespaces/my-org/jobs | jq .

# get run history for a specific task
curl http://localhost:5000/api/v1/namespaces/my-org/jobs/my-pipeline.read/runs | jq .
```

### Implementation files

| File | Purpose |
|---|---|
| `internal/lineage/lineage.go` | `Emitter` interface, `RunEvent` struct, `EventType` constants, `NewRunID()` |
| `internal/lineage/noop.go` | `NoopEmitter` — silently discards all events |
| `internal/lineage/http.go` | `HTTPEmitter` — POSTs JSON to `/api/v1/lineage` |
| `internal/lineage/factory.go` | `New(cfg LineageConfig) Emitter` — returns Noop or HTTP based on config |

## Observed CLI output

### `run` — success

```
▶  Running pipeline "csv-to-csv-test" (2 tasks)

time=... level=INFO msg="task starting" node=read
time=... level=INFO msg="task starting" node=write
time=... level=INFO msg="task finished" node=read  status=success records_in=5 records_out=5 records_dlq=0 duration=0s
  ✔  read                  in:5        out:5        dlq:0     0s
time=... level=INFO msg="task finished" node=write status=success records_in=5 records_out=5 records_dlq=0 duration=0s
  ✔  write                 in:5        out:5        dlq:0     0s

✅  Pipeline "csv-to-csv-test" finished successfully
```

### `connectors list`

```
TYPE          DESCRIPTION
------------  ----------------------------------------
csv           Read records from a local CSV file
              config: path, delimiter, has_header
csv-sink      Write records to a local CSV file
              config: path
postgres      Write records to a PostgreSQL table (upsert)
              config: dsn, table, upsert_key
```

### `validate` — valid pipeline

```
Validating pipeline.yaml

  ✔  schema valid
  ✔  2 connector(s), 1 transform(s), 3 task(s)
  ✔  DAG is acyclic

Execution order:
  1. read    [csv-source]
  2. filter  [(transform only) + transform:filter-active]
  3. write   [pg-sink]

✅  csv-filter-to-postgres is valid
```

### `validate` — error cases

```
Error: DAG error: pipeline DAG contains a cycle — check depends_on fields
Error: config error: task "read" references unknown connector "does-not-exist"
Error: config error: task "read" depends_on unknown task "ghost-task"
Error: config error: duplicate task id "read"
Error: config error: validation errors:
  - field "Version" is required
```

### `visualize`

```
✔  Pipeline graph written to active-customers.html
```

### `serve`

```
▶  Dashboard at http://localhost:8080  (pipeline executing)
   Press Ctrl+C to stop
```
Browser shows an interactive left-to-right DAG. With `--run`, nodes update live:
- gray → pending
- green → success
- red → failed

## Test coverage

| Package | Coverage | Tests | What they cover |
|---|---|---|---|
| `internal/agent` | 100% | 23 | `Parse` (9), `SystemPrompt` (8), `Generate` with mock LLM (7) — retry, error paths, context cancel |
| `internal/llm` | 92.9% | 23 | `OpenAIAdapter` HTTP mock (14), `BuildProvider` (8), `resolveEnv` (5) |
| `internal/lineage` | 88.1% | 20 | `NewRunID` (3), `NoopEmitter` (2), `HTTPEmitter` (11), `New()` factory (4) |
| `internal/engine` | 73.9% | 12 | Executor unit (7), `LocalEngine` integration (3), retry (2) |
| `internal/transform` | 79.3% | 9 | Filter pass/drop, uppercase map, cache hit, missing file, wrong entrypoint, country lookup (3) |
| `internal/connectors/csv` | 79.0% | 8 | Write+read, header sorting, multiple batches, empty batch, missing/invalid path, malformed record, numeric/bool |
| `internal/connectors/s3` | — | 11 unit + 3 integration | Unit: config validation, NDJSON/CSV buffering, multi-batch, empty batch, malformed record. Integration: skipped unless `TEST_S3_ENDPOINT` set (see LocalStack below) |
| `internal/dlq` | 85.7% | 3 | Push+count, drain, empty count |
| `internal/config` | 67.2% | 11 | Valid config, all required-field errors, duplicate names, broken refs, env substitution |
| `internal/dag` | 74.6% | 7 | Linear sort, diamond DAG, cycle, single node, duplicate node, predecessors, successors |

**Packages without automated tests:** `connectors/postgres` (requires live DB), `cmd/` (CLI integration), `codegen` (no logic).

### Running S3 integration tests with LocalStack

The S3 integration tests (`TestIntegration_S3ToS3_NDJSON`, `TestIntegration_S3ToS3_CSV`, `TestIntegration_Pipeline_S3ToS3_NDJSON`) are skipped by default. They require a running LocalStack instance:

```bash
# start LocalStack (free Community edition covers S3 fully)
docker run -p 4566:4566 localstack/localstack

# run only the S3 integration tests
TEST_S3_ENDPOINT=http://localhost:4566 go test ./internal/connectors/s3/... -v -run TestIntegration

# run the full suite (integration tests included)
TEST_S3_ENDPOINT=http://localhost:4566 go test ./...
```

The tests create two buckets (`eulantir-test`, `eulantir-pipeline-test`), upload fixture data, run the connectors or a full `LocalEngine` pipeline, and verify the output object in LocalStack. No real AWS credentials are needed — LocalStack accepts any non-empty key/secret pair.

## Key design decisions

- **Pipe-based task execution** — tasks connect via buffered `chan []Record` channels; source and sink run as concurrent goroutines, not sequentially.
- **At-least-once + idempotent sinks** — Postgres sink uses `ON CONFLICT DO UPDATE SET`; CSV sink is append-only.
- **Named return for `executor.run()`** — `defer func() { result.FinishedAt = time.Now() }()` works correctly only with a named return value; a value return copies before the defer runs.
- **Blank imports trigger connector registration** — `init()` in each connector package calls `connector.Default.RegisterSource/Sink`. Commands that need connectors must blank-import the packages. `cmd/generate.go` now also blank-imports connectors so the live catalog is available for the system prompt.
- **Transform source paths resolved relative to YAML file** — `config.Load()` converts relative `source` paths to absolute by joining with the YAML file's directory. This allows `pipeline.yaml` and its `generated/` folder to be moved as a unit without breaking paths.
- **Post-generation YAML validation** — `agent.Generate()` calls `config.LoadBytes()` after successfully parsing the LLM response. If the YAML fails structural validation, the error is fed back to the LLM as a correction message (same retry loop as parse errors).
- **Live connector catalog in system prompt** — `agent.New()` now accepts `[]connector.ConnectorInfo`; `SystemPrompt(catalog)` builds the connector section from the live registry. Always in sync with registered types.
- **Go plugin for transforms** — `go build -buildmode=plugin` compiles generated `.go` to a `.so` shared library; loaded at runtime with `plugin.Open()` + `plugin.Lookup()`. Full Go + any imports allowed. Linux/macOS only (Windows not supported — accepted constraint). Requires `go` toolchain at runtime. Plugin must be compiled with the same Go version as the host binary. Generated files must declare `package main` (plugin build mode requirement).
- **`SilenceUsage = true` on all `RunE`** — errors print one clean message, not the full help text.
- **Lineage is orthogonal** — `NoopEmitter` is used when lineage is disabled; the engine, executor, and connectors have no knowledge of lineage. `LocalEngine.Lineage` field is optional; nil → noop.
- **OpenLineage over plain HTTP** — no SDK dependency. Eulantir posts JSON to `/api/v1/lineage` using only `net/http`. Any OpenLineage-compatible backend (Marquez, DataHub, Atlan) works with a one-line config change.
- **Lineage errors are non-fatal** — `EmitStart`/`EmitComplete`/`EmitFail` failures log a warning and do not interrupt pipeline execution.

## Build phases

| Phase | Status | What it adds |
|---|---|---|
| 0 | ✅ Done | Skeleton — CLI, all interfaces |
| 1 | ✅ Done | LLM agent — `generate` with Ollama |
| 2 | ✅ Done | Config loader + DAG + `validate` — 18 tests |
| 3 | ✅ Done | Engine + CSV/Postgres + `run` + `connectors` — 25 tests |
| 4 | ✅ Done | Transform loading via Go plugin (`go build -buildmode=plugin` + `plugin.Open()`) — 6 new tests, e2e verified |
| 5 | ✅ Done | Full generate → validate → run loop (MVP) — live catalog, path resolution, post-gen validation, `--validate` / `--run` flags |
| 6 | ✅ Done | OpenLineage lineage + structured logging — `HTTPEmitter`, `NoopEmitter`, factory, wired into engine, 20 tests |
| 7 | ✅ Done | S3 source, Kafka source/sink, Anthropic Claude LLM adapter |
| 8 | ✅ Done | S3 sink (`s3-sink`) — NDJSON/CSV, `endpoint_url` for LocalStack/MinIO, 11 unit tests + 3 LocalStack integration tests |
| 9 | ✅ Done | Pipeline visualization — `visualize` (static HTML export) + `serve` (live SSE dashboard with optional `--run`) |

## Future improvements

Functional improvements identified for future phases. Grouped by area, with a recommended priority order at the end.

### Connectors

| Item | Description |
|---|---|
| ~~**S3 Sink**~~ | ✅ Done — see Phase 8. |
| **Postgres Source** | `SELECT` with cursor-based pagination or CDC via `pg_logical`. Unlocks DB-to-DB and DB-to-lake pipelines. Currently only a sink exists. |
| **HTTP Source/Sink** | Poll a REST API as a source; POST records to a webhook as a sink. Enables SaaS integrations (Stripe, HubSpot, etc.) without custom connectors. |
| **GCS / Azure Blob** | Cloud-parity alongside S3. Same streaming body pattern, different SDK (`cloud.google.com/go/storage`, `github.com/Azure/azure-sdk-for-go`). |

### Transform layer

| Item | Description |
|---|---|
| **Built-in transforms** | Declarative, no-code transforms in `pipeline.yaml` for common cases — `filter`, `rename`, `drop-nulls`, `cast`. Removes the biggest friction point for non-Go users. Example: `builtin: filter / when: "record.email != ''"`. |
| **Transform chaining** | A task currently supports one transform. Allow a list so small reusable transforms can be composed without writing one monolithic function. |
| **Python transforms** | Subprocess-based Python transform (stdin/stdout NDJSON). No Go plugin constraints; widens who can use Eulantir without requiring Go knowledge. |

### Pipeline orchestration

| Item | Description |
|---|---|
| **Scheduling** | Add a `schedule.cron` block to `pipeline.yaml` so Eulantir manages its own schedule rather than requiring external cron wiring. |
| **Fan-out to multiple sinks** | Allow `sinks: [a, b]` on a task to replicate records to multiple destinations in one pass without duplicating the source task. |
| **Checkpointing / resume** | Persist last committed offset per source to a checkpoint file. A failed pipeline restarts from the last checkpoint instead of from scratch. |

### Automatic metadata extraction and data cataloging

The goal is a fully automatic data catalog — no human annotation required. Three complementary mechanisms, designed to compose:

| Item | Description |
|---|---|
| **Statistical profiling at source nodes** | On the first batch of every source task, sample N records and compute: inferred types per field, null rate, cardinality estimate, min/max/mean for numerics, example values. Zero LLM cost, always-on, deterministic. Produces a `SchemaDatasetFacet` that can be emitted via the existing OpenLineage `HTTPEmitter` so Marquez/DataHub picks it up automatically. This is the foundation everything else builds on. |
| **LLM metadata enrichment at source nodes** | Optional, opt-in via `catalog.llm: true` in `pipeline.yaml`. After statistical profiling, send the sampled schema + example values to an LLM (reusing the existing `llm.Provider` abstraction) and ask it to produce: field-level descriptions, semantic types (e.g. `email`, `currency`, `country_code`), PII flags, and a dataset summary. Results stored as an `DocumentationDatasetFacet` in the lineage event. Only triggered on first run for a dataset or when the schema changes — not on every run. |
| **Schema-change detection** | Compare the current run's inferred schema against the last emitted `SchemaDatasetFacet`. If fields are added, removed, or change type, emit a `SchemaChangeRunFacet` and optionally route records to the DLQ. Catches upstream schema drift without requiring a declared contract. |
| **Column-level lineage** | Track which source fields survive into sink records through transforms. Requires transform introspection (or a lightweight AST pass on generated `.go` files). Emits `ColumnLineageDatasetFacet` — the richest catalog signal. Most useful once built-in transforms exist, since generated transform code is harder to introspect reliably. |

**Recommended sequencing:** statistical profiling first (cheap, no dependencies) → schema-change detection (reuses profiling output) → LLM enrichment (leverages both) → column-level lineage (builds on all three).

**Architecture fit:** profiling and schema-change detection run inside the engine alongside existing lineage emission — orthogonal to task execution, zero impact on pipelines that disable them. LLM enrichment reuses `internal/llm` and `internal/agent` unchanged. All catalog output flows through the existing `HTTPEmitter` as OpenLineage facets — no new backend required.

**Config sketch:**
```yaml
catalog:
  enabled: true
  sample_size: 100          # records to sample per source task
  llm_enrichment: true      # send schema to LLM for semantic annotations
  on_schema_change: dlq     # dlq | warn | fail  (what to do when schema drifts)
```

### Observability

| Item | Description |
|---|---|
| **Prometheus metrics** | Expose `/metrics` while a pipeline runs: `records_processed_total`, `records_dlq_total`, `task_duration_seconds`, `batch_size`. Lineage captures history; metrics capture real-time throughput. |
| ~~**Pipeline status API**~~ | ✅ Done — `eulantir serve` provides a live HTTP dashboard with SSE task-status updates. |
| **Alerting hooks** | A `notifications` block in `pipeline.yaml` to POST to a Slack webhook or PagerDuty on failure. Right now failures are silent unless you watch the terminal. |

### Developer experience

| Item | Description |
|---|---|
| **`--dry-run` flag** | Validate config, build the DAG, open connectors, read one batch from each source, but write nothing. Confirms credentials and data shape before a real run. |
| **`eulantir diff`** | Show structural differences between two pipeline configs (added/removed tasks, changed connectors). Useful in code review. |
| **Record schema validation** | Declare an expected JSON schema per connector; the engine DLQs records that don't match. Catches upstream schema drift early. |
| **Hot-reload transforms** | Watch `generated/` for changes; recompile and swap the plugin without restarting the pipeline. Shortens the edit→test loop during development. |

### Recommended priority order

1. **Built-in transforms** — removes the biggest friction for non-Go users; covers 80% of real pipeline logic with zero code.
2. **Scheduling** — makes Eulantir self-contained; without it every deployment needs an external cron.
3. **Postgres source** — unlocks the most common real-world pattern (relational DB → data lake / analytics sink).
4. **`--dry-run` flag** — low implementation cost, high day-to-day value for anyone authoring or debugging pipelines.
5. **Statistical profiling at source nodes** — zero LLM cost, always-on foundation for automatic cataloging; also enables schema-change detection.
6. **LLM metadata enrichment** — highest-value catalog feature once profiling is in place; reuses existing `llm.Provider` with no new infrastructure.

---

## Local development setup

Ollama must be running for `generate`:

```bash
brew install ollama && brew services start ollama && ollama pull llama3.2
curl http://localhost:11434/api/tags   # verify
```
