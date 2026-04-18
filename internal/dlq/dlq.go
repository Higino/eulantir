package dlq

import (
	"context"

	"github.com/higino/eulantir/internal/connector"
)

// DLQ stores records that have exhausted all retry attempts.
// Each pipeline node gets its own named queue.
type DLQ interface {
	// Push adds a failed record and the reason it failed.
	Push(ctx context.Context, nodeID string, r connector.Record, reason error) error
	// Drain returns a channel of all records stored for nodeID.
	Drain(ctx context.Context, nodeID string) (<-chan connector.Record, error)
	// Count returns how many records are stored for nodeID.
	Count(ctx context.Context, nodeID string) (int64, error)
}
