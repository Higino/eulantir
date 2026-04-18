package lineage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

const (
	openLineagePath = "/api/v1/lineage"
	producerURL     = "https://github.com/higino/eulantir"
)

// HTTPEmitter posts OpenLineage RunEvents to a remote HTTP backend
// (e.g. Marquez at http://localhost:5000).
type HTTPEmitter struct {
	endpoint   string
	namespace  string
	httpClient *http.Client
}

// NewHTTPEmitter returns an emitter that sends events to endpoint.
// namespace is used as the default job namespace when the event has none.
func NewHTTPEmitter(endpoint, namespace string) *HTTPEmitter {
	return &HTTPEmitter{
		endpoint:  endpoint,
		namespace: namespace,
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// EmitStart posts a START event.
func (e *HTTPEmitter) EmitStart(ctx context.Context, evt RunEvent) error {
	evt.EventType = EventTypeStart
	return e.post(ctx, evt)
}

// EmitComplete posts a COMPLETE event.
func (e *HTTPEmitter) EmitComplete(ctx context.Context, evt RunEvent) error {
	evt.EventType = EventTypeComplete
	return e.post(ctx, evt)
}

// EmitFail posts a FAIL event.
func (e *HTTPEmitter) EmitFail(ctx context.Context, evt RunEvent) error {
	evt.EventType = EventTypeFail
	return e.post(ctx, evt)
}

// post marshals evt to JSON and POSTs it to the lineage endpoint.
func (e *HTTPEmitter) post(ctx context.Context, evt RunEvent) error {
	// Fill in defaults.
	if evt.Namespace == "" {
		evt.Namespace = e.namespace
	}
	if evt.ProducerID == "" {
		evt.ProducerID = producerURL
	}
	if evt.OccurredAt.IsZero() {
		evt.OccurredAt = time.Now()
	}

	b, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal lineage event: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		e.endpoint+openLineagePath, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("create lineage request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("send lineage event: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("lineage backend returned HTTP %d", resp.StatusCode)
	}
	return nil
}
