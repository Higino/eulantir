# Eulantir — Agent Handoff Memory

> Give this file (along with CLAUDE.md and README.md) to any new agent continuing this project.

---

## Who is the user

- **Name**: Higino Silva  
- **Email**: higino.silva@bloq.it  
- **GitHub handle**: higino  
- **Go module**: `github.com/higino/eulantir`  
- **Project root**: `/Users/higinosilva/Documents/Code/HiginoGitHub/Eulantir`

---

## What is Eulantir

A generic data pipeline orchestrator written in Go with two jobs:

1. **Generate** — takes plain-English intent, calls an LLM, writes `pipeline.yaml` + Go transform files to disk.
2. **Run** — parses the YAML, builds a DAG, loads transform code at runtime (yaegi), executes tasks in dependency order with retries and a dead-letter queue.

The LLM only runs at generation time. Once files are on disk, pipeline execution is pure Go with no LLM calls.

---

## Development plan — all phases

| Phase | Feature | Status |
|-------|---------|--------|
| **0** | CLI skeleton + all interfaces (cobra, Provider, Source, Sink, Transform, DLQ, Lineage) | ✅ Done |
| **1** | LLM agent — `generate` command (Ollama + OpenAI adapters, retry loop, YAML+Go parser) | ✅ Done |
| **2** | Config loader + DAG validator — `validate` command (Kahn's sort, cycle detection, 18 tests) | ✅ Done |
| **3** | Pipeline engine + CSV/Postgres connectors — `run` + `connectors` commands (25 tests, e2e verified) | ✅ Done |
| **4** | Transform loading via **Go plugin** — compile generated `.go` to `.so`, load with `plugin.Open()` — 6 tests, e2e verified | ✅ Done |
| **5** | Full generate → validate → run loop (MVP) — live catalog, path resolution, post-gen validation, `--validate`/`--run` flags | ✅ Done |
| **6** | OpenLineage lineage events + structured logging | ✅ Done |
| **7** | S3 source, Kafka source/sink, Anthropic Claude LLM adapter | 🔜 **NEXT** |

---

## Where we stopped

**Last completed work**: Phase 6 is fully done and verified. All tests pass.

**Next task**: Implement **Phase 7 — S3 source, Kafka source/sink, Anthropic Claude LLM adapter**.

---

## Phase 6 — Lineage (completed)

### What was built

| File | Purpose |
|---|---|
| `internal/lineage/lineage.go` | `Emitter` interface, `RunEvent` struct, `EventType` constants (`START`/`COMPLETE`/`FAIL`), `NewRunID()` UUID generator |
| `internal/lineage/noop.go` | `NoopEmitter` — silently discards all events; zero overhead |
| `internal/lineage/http.go` | `HTTPEmitter` — POSTs OpenLineage JSON events to `/api/v1/lineage` |
| `internal/lineage/factory.go` | `New(cfg LineageConfig) Emitter` — returns Noop or HTTP based on config |
| `internal/lineage/lineage_test.go` | 20 tests — all emitters, factory, UUID format, HTTP error paths |

`internal/engine/engine.go` was updated to:
- Add `Lineage lineage.Emitter` field to `LocalEngine` (nil → NoopEmitter)
- Emit `EmitStart` before each task, `EmitComplete`/`EmitFail` after
- Include `recordsIn`/`recordsOut`/`recordsDLQ` in `JobFacets`
- Add `pipeline starting` / `pipeline finished` slog lines

`cmd/run.go` was updated to build `lineage.New(cfg.Lineage)` and pass it to the engine.

### OpenLineage and Marquez

**OpenLineage** — CNCF open standard (Apache 2.0) for lineage events. A specification, not a product. Eulantir implements it using only `net/http` + `encoding/json`. No SDK dependency.

**Marquez** — the reference implementation and recommended self-hosted backend. Fully open source (Apache 2.0), no paid tier.

```bash
# start Marquez (API + UI)
docker run -p 5000:5000 -p 5001:5001 marquezproject/marquez
# API: http://localhost:5000
# UI:  http://localhost:5001
```

Because Eulantir speaks the standard OpenLineage wire format, any compatible backend works — Marquez, DataHub, Atlan, Apache Atlas — with a one-line endpoint change in `pipeline.yaml`.

### Enabling lineage

```yaml
lineage:
  enabled: true
  endpoint: http://localhost:5000   # Marquez or any OpenLineage-compatible backend
  namespace: my-org                 # groups pipelines in the backend UI
```

When `enabled: false` or endpoint is empty → `NoopEmitter`, zero HTTP calls, pipeline unaffected.

### Key design decisions (lineage)

- **Orthogonal to execution** — engine, executor, and connectors have no knowledge of lineage; it's wired only in `engine.go` around the task goroutine
- **Non-fatal** — `EmitStart`/`EmitComplete`/`EmitFail` failures log a warning and do not interrupt the pipeline
- **No external dependency** — plain `net/http` + `encoding/json`; Marquez is an external service, not a Go module

---

## Architecture summary

### Data flow (pipe-based)

```
source goroutine                    sink goroutine
┌──────────────────┐  chan []Record ┌──────────────────┐
│ src.ReadBatch()  │ ─────────────▶ │ sink.WriteBatch() │
└──────────────────┘                └──────────────────┘
```

After Phase 4, transform nodes sit between source and sink:

```
source → chan → [transform.Apply() per record] → chan → sink
```

### Package map

```
cmd/                   CLI (cobra) — no business logic
internal/
  agent/               LLM agent: prompt, parser, retry loop
  llm/                 Provider interface + OpenAIAdapter (Ollama + OpenAI share one)
  config/              PipelineConfig structs, Load(), $ENV substitution, validation
  dag/                 DAG + Kahn's topological sort + cycle detection
  engine/              LocalEngine, pipe-based executor, TaskResult, retry, DLQ wiring
  connector/           Record, Source, Sink interfaces + Default registry singleton
  connectors/csv/      CSV source + sink (register via init())
  connectors/postgres/ Postgres upsert sink (pgx batch, ON CONFLICT DO UPDATE)
  transform/           Transform interface + Go plugin loader (compile .go → .so, cache by hash)
  dlq/                 FileDLQ: NDJSON per node per day
  lineage/             Emitter interface + NoopEmitter + HTTPEmitter + New() factory (OpenLineage)
  codegen/             GeneratedPipeline struct (Phase 5)
testdata/pipelines/    valid.yaml, cycle.yaml
```

### Connector registry heuristic (executor.resolveConnectors)

- `postgres`, `csv-sink` → **sink**
- everything else (`csv`, `s3`, `kafka`, …) → **source**

### LLM provider abstraction

- Ollama and OpenAI share `OpenAIAdapter` (both speak `/v1/chat/completions`)
- `NewOllamaAdapter("")` → `http://localhost:11434`, API key = `"ollama"`
- `NewOpenAIAdapter(apiKey)` → `https://api.openai.com`
- Anthropic adapter is Phase 7

### Generated transform contract (Go plugin)

```go
// transform: my_transform          ← required first-line header
package main                        // MUST be "main" — plugin build mode requirement

import (
    "context"
    "encoding/json"
    "github.com/higino/eulantir/internal/connector"
)

// Return nil, nil                      → drop record (filter)
// Return []connector.Record{in}, nil   → pass through or modify (map)
// Return []connector.Record{a, b}, nil → fanout
func MyTransform(ctx context.Context, in connector.Record) ([]connector.Record, error) {
    var row map[string]any
    json.Unmarshal(in.Value, &row)
    // your logic here
    return []connector.Record{in}, nil
}
```

---

## Known design decisions (important gotchas)

| Decision | Why |
|----------|-----|
| Named return in `executor.run()` → `(result TaskResult)` | `defer func() { result.FinishedAt = time.Now() }()` only works with named return — value return copies before defer fires |
| Blank imports in `cmd/run.go` and `cmd/connectors.go` | `init()` in each connector package calls `connector.Default.Register*`. Commands must import connector packages to trigger registration |
| `SilenceUsage = true` on every `RunE` | Prevents cobra from printing the full usage block on every error |
| `os.ExpandEnv` on raw YAML before `yaml.Unmarshal` | Secrets stay out of YAML; `$POSTGRES_DSN` → value from env |
| Pipe buffer = 10 batches | Source and sink run as concurrent goroutines; buffer prevents constant blocking |
| Go plugin for transforms | `go build -buildmode=plugin` compiles generated `.go` to a `.so`; loaded with `plugin.Open()` + `plugin.Lookup()`; Windows NOT supported (acceptable — target is Linux/macOS); requires `go` toolchain at runtime; plugin must match host Go version |
| Lineage is orthogonal | `NoopEmitter` when disabled; engine/executor/connectors have no lineage knowledge; `LocalEngine.Lineage` nil → noop |
| Lineage errors are non-fatal | `Emit*` failures log a warning and do not stop pipeline execution |
| No lineage SDK dependency | Eulantir posts plain JSON over `net/http`; Marquez is an external service, not a Go module |

---

## Test coverage (as of Phase 6)

| Package | Coverage | Tests | What they cover |
|---------|----------|-------|-----------------|
| `internal/agent` | 100% | 23 | Parse (9), SystemPrompt (8), Generate with mock LLM (7) — retry, error paths, context cancel |
| `internal/llm` | 92.9% | 23 | OpenAIAdapter HTTP (14), BuildProvider (8), resolveEnv (5) |
| `internal/lineage` | 88.1% | 20 | NewRunID (3), NoopEmitter (2), HTTPEmitter (11), factory New() (4) |
| `internal/engine` | 73.9% | 12 | Executor unit (7), LocalEngine integration (3), retry (2) |
| `internal/transform` | 79.3% | 9 | Filter pass/drop, uppercase map, cache hit, missing file, wrong entrypoint, country lookup (3) |
| `internal/connectors/csv` | 79.0% | 8 | Write+read, header sorting, multiple batches, empty batch, missing path, invalid path, malformed record, numeric/bool |
| `internal/dlq` | 85.7% | 3 | Push+count, drain, empty count |
| `internal/config` | 67.2% | 11 | Valid config, all required-field errors, duplicate names, broken refs, env substitution |
| `internal/dag` | 74.6% | 7 | Linear sort, diamond DAG, cycle, single node, duplicate, predecessors, successors |

---

## CLI commands

```bash
go run main.go generate "<intent>"
go run main.go generate --provider ollama --model llama3.2 --output ./my-pipeline "<intent>"
go run main.go validate pipeline.yaml
go run main.go run pipeline.yaml
go run main.go connectors list
go run main.go connectors info csv
```

### Generate flags

```
--provider   ollama | openai | anthropic   (default: ollama)
--model      model name                     (default: llama3.2)
--output     output directory               (default: .)
--base-url   override LLM endpoint URL
--validate   validate the pipeline immediately after generation
--run        validate + run the pipeline immediately after generation
-v           verbose (token counts, retries)
```

---

## Local dev setup

```bash
# Ollama required for generate command
brew install ollama && brew services start ollama && ollama pull llama3.2
curl http://localhost:11434/api/tags   # verify Ollama is running

# Marquez required for lineage (optional — pipelines run fine without it)
docker run -p 5000:5000 -p 5001:5001 marquezproject/marquez
# API: http://localhost:5000
# UI:  http://localhost:5001

# Build and test
cd /Users/higinosilva/Documents/Code/HiginoGitHub/Eulantir
go build ./...
go test ./...
```
