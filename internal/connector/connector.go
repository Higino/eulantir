package connector

import "context"

// Record is the atomic unit of data flowing through a pipeline.
// Key is optional (used for partitioning/dedup). Value is the payload.
// Offset is the source-assigned position, used for at-least-once tracking.
type Record struct {
	Key     []byte
	Value   []byte
	Headers map[string]string
	Offset  int64
}

// Connector is the base capability shared by all sources and sinks.
type Connector interface {
	// Name returns the unique registered name of this connector type.
	Name() string
	// Open initialises the connection using the provided config map.
	Open(ctx context.Context, cfg map[string]any) error
	// Close releases all resources held by the connector.
	Close(ctx context.Context) error
}

// Source reads batches of records from an external system.
// ReadBatch blocks until at least one record is available or ctx is done.
// Returns io.EOF when the source is fully exhausted (batch pipelines only).
// Commit acknowledges that all records up to offset have been processed.
type Source interface {
	Connector
	ReadBatch(ctx context.Context, maxSize int) ([]Record, error)
	Commit(ctx context.Context, offset int64) error
}

// Sink writes batches of records to an external system.
// WriteBatch must be idempotent: re-sending the same record offsets must
// not produce duplicate rows (use upsert keys or dedup logic internally).
type Sink interface {
	Connector
	WriteBatch(ctx context.Context, records []Record) error
}
