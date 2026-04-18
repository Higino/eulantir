package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/dag"
	"github.com/higino/eulantir/internal/dlq"
	"github.com/higino/eulantir/internal/transform"
)

// Batch is the unit flowing between tasks over a pipe channel.
type Batch = []connector.Record

// executor runs a single task node.
// Data flows via typed channels:
//   - source node: reads from connector  → writes to outPipe
//   - sink   node: reads from inPipe     → writes to connector
//   - transform-only node (Phase 4): reads from inPipe → transforms → writes to outPipe
type executor struct {
	registry  connectorRegistry
	dlq       dlq.DLQ
	retryCfg  config.RetryConfig
	batchSize int
}

// connectorRegistry is the subset of connector.Registry used by the executor.
type connectorRegistry interface {
	BuildSource(typ string, cfg map[string]any) (connector.Source, error)
	BuildSink(typ string, cfg map[string]any) (connector.Sink, error)
	IsSink(typ string) bool
}

// run executes one task and closes outPipe when done.
// inPipe may be nil for source nodes; outPipe may be nil for sink nodes.
func (e *executor) run(
	ctx context.Context,
	node dag.Node,
	pCfg config.PipelineConfig,
	inPipe <-chan Batch,
	outPipe chan<- Batch,
) (result TaskResult) {
	result = TaskResult{
		NodeID:    node.ID,
		Status:    StatusRunning,
		StartedAt: time.Now(),
	}
	// named return: defer captures the return variable, so FinishedAt is
	// always set correctly regardless of which return path is taken.
	defer func() { result.FinishedAt = time.Now() }()

	if outPipe != nil {
		defer close(outPipe)
	}

	srcCfg, sinkCfg, err := e.resolveConnectors(node, pCfg)
	if err != nil {
		result.Status = StatusFailed
		result.Err = err
		return result
	}

	batchSize := e.batchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	switch {
	case srcCfg != nil && sinkCfg == nil:
		// ── SOURCE NODE ──────────────────────────────────────────────
		// Read from connector, send each batch downstream via outPipe.
		src, err := e.registry.BuildSource(srcCfg.Type, srcCfg.Config)
		if err != nil {
			result.Status = StatusFailed
			result.Err = fmt.Errorf("open source: %w", err)
			return result
		}
		defer src.Close(ctx)

		for {
			var batch Batch
			readErr := backoff.Retry(func() error {
				b, err := src.ReadBatch(ctx, batchSize)
				if err == io.EOF {
					return backoff.Permanent(io.EOF)
				}
				if err != nil {
					slog.Warn("read batch error, retrying", "node", node.ID, "err", err)
					return err
				}
				batch = b
				return nil
			}, backoff.WithContext(newBackoff(e.retryCfg), ctx))

			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				result.Status = StatusFailed
				result.Err = fmt.Errorf("read: %w", readErr)
				return result
			}

			result.RecordsIn += int64(len(batch))
			result.RecordsOut += int64(len(batch))

			if outPipe != nil {
				select {
				case outPipe <- batch:
				case <-ctx.Done():
					result.Status = StatusFailed
					result.Err = ctx.Err()
					return result
				}
			}

			if len(batch) > 0 {
				_ = src.Commit(ctx, batch[len(batch)-1].Offset)
			}
		}

	case sinkCfg != nil && srcCfg == nil:
		// ── SINK NODE ────────────────────────────────────────────────
		// Receive batches from inPipe, write to connector.
		sink, err := e.registry.BuildSink(sinkCfg.Type, sinkCfg.Config)
		if err != nil {
			result.Status = StatusFailed
			result.Err = fmt.Errorf("open sink: %w", err)
			return result
		}
		defer sink.Close(ctx)

		for batch := range inPipe {
			result.RecordsIn += int64(len(batch))

			writeErr := backoff.Retry(func() error {
				if err := sink.WriteBatch(ctx, batch); err != nil {
					slog.Warn("write batch error, retrying", "node", node.ID, "err", err)
					return err
				}
				return nil
			}, backoff.WithContext(newBackoff(e.retryCfg), ctx))

			if writeErr != nil {
				if e.dlq != nil {
					for _, r := range batch {
						_ = e.dlq.Push(ctx, node.ID, r, writeErr)
					}
				}
				result.RecordsDLQ += int64(len(batch))
				slog.Error("write exhausted retries → DLQ",
					"node", node.ID, "records", len(batch), "err", writeErr)
				continue
			}

			result.RecordsOut += int64(len(batch))
		}

	default:
		// ── TRANSFORM-ONLY NODE ──────────────────────────────────────
		if node.TransformRef == "" {
			slog.Warn("node has no connector or transform ref — skipping", "node", node.ID)
			result.Status = StatusSkipped
			return result
		}

		tCfg, err := e.resolveTransform(node, pCfg)
		if err != nil {
			result.Status = StatusFailed
			result.Err = err
			return result
		}

		tr, err := transform.Load(tCfg.Name, tCfg.Source, tCfg.Entrypoint)
		if err != nil {
			result.Status = StatusFailed
			result.Err = fmt.Errorf("load transform %q: %w", tCfg.Name, err)
			return result
		}

		for batch := range inPipe {
			result.RecordsIn += int64(len(batch))

			var outBatch []connector.Record
			for _, rec := range batch {
				outs, applyErr := tr.Apply(ctx, rec)
				if applyErr != nil {
					if e.dlq != nil {
						_ = e.dlq.Push(ctx, node.ID, rec, applyErr)
					}
					result.RecordsDLQ++
					slog.Warn("transform error → DLQ",
						"node", node.ID, "err", applyErr)
					continue
				}
				outBatch = append(outBatch, outs...)
			}

			result.RecordsOut += int64(len(outBatch))

			if len(outBatch) > 0 && outPipe != nil {
				select {
				case outPipe <- outBatch:
				case <-ctx.Done():
					result.Status = StatusFailed
					result.Err = ctx.Err()
					return result
				}
			}
		}
	}

	result.Status = StatusSuccess
	return result
}

// resolveTransform looks up the TransformConfig referenced by node.TransformRef.
func (e *executor) resolveTransform(node dag.Node, pCfg config.PipelineConfig) (*config.TransformConfig, error) {
	for i := range pCfg.Transforms {
		if pCfg.Transforms[i].Name == node.TransformRef {
			return &pCfg.Transforms[i], nil
		}
	}
	return nil, fmt.Errorf("transform %q not found in config", node.TransformRef)
}

// resolveConnectors returns the source and/or sink ConnectorConfig for a node.
func (e *executor) resolveConnectors(node dag.Node, pCfg config.PipelineConfig) (src *config.ConnectorConfig, sink *config.ConnectorConfig, err error) {
	if node.ConnectorRef == "" {
		return nil, nil, nil
	}

	var found *config.ConnectorConfig
	for i := range pCfg.Connectors {
		if pCfg.Connectors[i].Name == node.ConnectorRef {
			found = &pCfg.Connectors[i]
			break
		}
	}
	if found == nil {
		return nil, nil, fmt.Errorf("connector %q not found", node.ConnectorRef)
	}

	if e.registry.IsSink(found.Type) {
		return nil, found, nil
	}
	return found, nil, nil
}
