package lineage

import "context"

// NoopEmitter discards all events silently.
// It is used when lineage.enabled = false or no endpoint is configured.
type NoopEmitter struct{}

func (NoopEmitter) EmitStart(_ context.Context, _ RunEvent) error    { return nil }
func (NoopEmitter) EmitComplete(_ context.Context, _ RunEvent) error { return nil }
func (NoopEmitter) EmitFail(_ context.Context, _ RunEvent) error     { return nil }
