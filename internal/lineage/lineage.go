package lineage

import (
	"context"
	"crypto/rand"
	"fmt"
	"time"
)

// EventType is the lifecycle stage of a run event.
type EventType string

const (
	EventTypeStart    EventType = "START"
	EventTypeComplete EventType = "COMPLETE"
	EventTypeFail     EventType = "FAIL"
)

// Dataset is an OpenLineage-compatible dataset reference.
type Dataset struct {
	Namespace string         `json:"namespace"`
	Name      string         `json:"name"`
	Facets    map[string]any `json:"facets,omitempty"`
}

// RunEvent carries the metadata emitted at the start, completion, or
// failure of a task node. It follows the OpenLineage RunEvent schema.
type RunEvent struct {
	EventType  EventType      `json:"eventType"`
	RunID      string         `json:"runId"`
	JobName    string         `json:"jobName"`
	Namespace  string         `json:"namespace,omitempty"`
	JobFacets  map[string]any `json:"jobFacets,omitempty"`
	Inputs     []Dataset      `json:"inputs,omitempty"`
	Outputs    []Dataset      `json:"outputs,omitempty"`
	ProducerID string         `json:"producer"`
	OccurredAt time.Time      `json:"eventTime"`
	// Error is set on FAIL events.
	Error string `json:"error,omitempty"`
}

// Emitter sends OpenLineage-compatible run events to a backend
// (e.g. Marquez, Atlan, or a custom HTTP endpoint).
type Emitter interface {
	EmitStart(ctx context.Context, evt RunEvent) error
	EmitComplete(ctx context.Context, evt RunEvent) error
	EmitFail(ctx context.Context, evt RunEvent) error
}

// NewRunID generates a random UUID v4 string.
// Used to correlate START / COMPLETE / FAIL events for the same task run.
func NewRunID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// fallback: use current time as hex (very unlikely)
		return fmt.Sprintf("%x", time.Now().UnixNano())
	}
	// Set UUID version 4 and variant bits.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
