package cmd

import (
	"fmt"

	"github.com/higino/eulantir/internal/config"
)

// buildNodeLabel returns a human-readable bracket label for a DAG node,
// used in the validate and generate --validate output.
func buildNodeLabel(connectorRef, transformRef string, cfg *config.PipelineConfig) string {
	connector := connectorRef
	if connector == "" {
		connector = "(transform only)"
	}
	transform := ""
	if transformRef != "" {
		// find the short transform name for display
		for _, t := range cfg.Transforms {
			if t.Name == transformRef {
				transform = fmt.Sprintf(" + transform:%s", t.Name)
				break
			}
		}
	}
	return connector + transform
}
