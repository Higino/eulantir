package lineage

import "github.com/higino/eulantir/internal/config"

// New creates the appropriate Emitter from the pipeline's lineage config.
//
//   - If lineage.enabled = false or lineage.endpoint is empty → NoopEmitter
//   - Otherwise → HTTPEmitter pointed at lineage.endpoint
func New(cfg config.LineageConfig) Emitter {
	if !cfg.Enabled || cfg.Endpoint == "" {
		return NoopEmitter{}
	}
	ns := cfg.Namespace
	if ns == "" {
		ns = "eulantir"
	}
	return NewHTTPEmitter(cfg.Endpoint, ns)
}
