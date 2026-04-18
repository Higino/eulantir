package cmd

import (
	"fmt"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/dlq"
	"github.com/higino/eulantir/internal/engine"
	"github.com/higino/eulantir/internal/lineage"
)

// buildNodeLabel returns a human-readable bracket label for a DAG node,
// used in the validate and generate --validate output.
func buildNodeLabel(connectorRef, transformRef string) string {
	connector := connectorRef
	if connector == "" {
		connector = "(transform only)"
	}
	transform := ""
	if transformRef != "" {
		transform = fmt.Sprintf(" + transform:%s", transformRef)
	}
	return connector + transform
}

// buildDLQ constructs a FileDLQ from the pipeline DLQ config, or returns nil
// if DLQ is disabled.
func buildDLQ(cfg config.PipelineConfig) dlq.DLQ {
	if !cfg.DLQ.Enabled {
		return nil
	}
	dir := "./dlq"
	if p, ok := cfg.DLQ.Config["path"].(string); ok && p != "" {
		dir = p
	}
	return dlq.NewFileDLQ(dir)
}

// buildEngine constructs a LocalEngine wired with the default connector
// registry, DLQ (if enabled), and lineage emitter.
func buildEngine(cfg config.PipelineConfig) *engine.LocalEngine {
	return &engine.LocalEngine{
		Registry: connector.Default,
		DLQ:      buildDLQ(cfg),
		Lineage:  lineage.New(cfg.Lineage),
	}
}
