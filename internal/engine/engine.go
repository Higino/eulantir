package engine

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/dag"
	"github.com/higino/eulantir/internal/dlq"
	"github.com/higino/eulantir/internal/lineage"
)

// RunStatus describes the current state of a task node execution.
type RunStatus string

const (
	StatusPending RunStatus = "pending"
	StatusRunning RunStatus = "running"
	StatusSuccess RunStatus = "success"
	StatusFailed  RunStatus = "failed"
	StatusSkipped RunStatus = "skipped"
)

// TaskResult is emitted on the results channel after each task completes.
type TaskResult struct {
	NodeID     string
	Status     RunStatus
	RecordsIn  int64
	RecordsOut int64
	RecordsDLQ int64
	StartedAt  time.Time
	FinishedAt time.Time
	Err        error
}

// Engine orchestrates a full pipeline run.
type Engine interface {
	Run(ctx context.Context, cfg config.PipelineConfig) (<-chan TaskResult, error)
}

// LocalEngine runs all tasks on the local machine.
// Tasks are connected by buffered channels (pipes) so data streams between
// them concurrently — source, transform, and sink nodes all run in parallel.
type LocalEngine struct {
	Registry *connector.Registry
	DLQ      dlq.DLQ
	// Lineage is an optional OpenLineage emitter. When nil a no-op is used.
	Lineage lineage.Emitter
}

// lineageEmitter returns the configured Emitter or a NoopEmitter if none is set.
func (eng *LocalEngine) lineageEmitter() lineage.Emitter {
	if eng.Lineage != nil {
		return eng.Lineage
	}
	return lineage.NoopEmitter{}
}

// Run builds the DAG, creates inter-task pipes, then launches each task as a
// goroutine. Returns a channel that emits one TaskResult per task and closes
// when every task has finished (or ctx is cancelled).
func (eng *LocalEngine) Run(ctx context.Context, cfg config.PipelineConfig) (<-chan TaskResult, error) {
	_, sorted, err := dag.Build(cfg.Tasks)
	if err != nil {
		return nil, err
	}

	batchSize := cfg.Execution.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	slog.Info("pipeline starting",
		"name", cfg.Name,
		"tasks", len(sorted),
		"batch_size", batchSize,
	)

	// Create one buffered pipe channel per task.
	// A task's output pipe is the input pipe of its direct successor(s).
	// Buffer size = 10 batches so source and sink don't always block each other.
	const pipeBuf = 10
	pipes := make(map[string]chan Batch, len(sorted))
	for _, n := range sorted {
		pipes[n.ID] = make(chan Batch, pipeBuf)
	}

	results := make(chan TaskResult, len(sorted))
	emitter := eng.lineageEmitter()
	exec := &executor{
		registry:  eng.Registry,
		dlq:       eng.DLQ,
		retryCfg:  cfg.Retry,
		batchSize: batchSize,
	}

	var wg sync.WaitGroup
	for _, node := range sorted {
		// inPipe: output channel of this node's single dependency (if any).
		// For fan-in (multiple depends_on) we merge in a future phase.
		var inPipe <-chan Batch
		if len(node.DependsOn) > 0 {
			inPipe = pipes[node.DependsOn[0]]
		}

		// outPipe: this node's own channel, read by its downstream successor.
		outPipe := pipes[node.ID]

		wg.Add(1)
		go func(n dag.Node, in <-chan Batch, out chan<- Batch) {
			defer wg.Done()

			// Build the lineage event skeleton shared across START/COMPLETE/FAIL.
			ns := cfg.Lineage.Namespace
			if ns == "" {
				ns = "eulantir"
			}
			runID := lineage.NewRunID()
			baseEvt := lineage.RunEvent{
				RunID:      runID,
				JobName:    fmt.Sprintf("%s.%s", cfg.Name, n.ID),
				Namespace:  ns,
				ProducerID: "https://github.com/higino/eulantir",
				OccurredAt: time.Now(),
			}

			if err := emitter.EmitStart(ctx, baseEvt); err != nil {
				slog.Warn("lineage EmitStart failed", "node", n.ID, "err", err)
			}

			slog.Info("task starting", "node", n.ID)
			result := exec.run(ctx, n, cfg, in, out)
			slog.Info("task finished",
				"node", n.ID,
				"status", result.Status,
				"records_in", result.RecordsIn,
				"records_out", result.RecordsOut,
				"records_dlq", result.RecordsDLQ,
				"duration", result.FinishedAt.Sub(result.StartedAt).Round(time.Millisecond),
			)

			// Emit completion or failure event with execution stats.
			doneEvt := baseEvt
			doneEvt.OccurredAt = result.FinishedAt
			doneEvt.JobFacets = map[string]any{
				"recordsIn":  result.RecordsIn,
				"recordsOut": result.RecordsOut,
				"recordsDLQ": result.RecordsDLQ,
			}

			if result.Status == StatusFailed {
				if result.Err != nil {
					doneEvt.Error = result.Err.Error()
				}
				if err := emitter.EmitFail(ctx, doneEvt); err != nil {
					slog.Warn("lineage EmitFail failed", "node", n.ID, "err", err)
				}
			} else {
				if err := emitter.EmitComplete(ctx, doneEvt); err != nil {
					slog.Warn("lineage EmitComplete failed", "node", n.ID, "err", err)
				}
			}

			results <- result
		}(node, inPipe, outPipe)
	}

	// Close results when all tasks are done.
	go func() {
		wg.Wait()
		slog.Info("pipeline finished", "name", cfg.Name)
		close(results)
	}()

	return results, nil
}
