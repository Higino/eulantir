package transform

import (
	"context"

	"github.com/higino/eulantir/internal/connector"
)

// Transform is applied to every record passing through a task node.
// It may return zero records (filter), one (map), or many (flatmap).
// Implementations are loaded at runtime from generated Go source files.
type Transform interface {
	// Name returns the unique name of this transform, matching TransformConfig.Name.
	Name() string
	// Apply processes a single record and returns zero or more output records.
	Apply(ctx context.Context, in connector.Record) ([]connector.Record, error)
}
