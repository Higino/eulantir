package lineage_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/lineage"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// captureServer starts a test HTTP server that stores the last received body.
func captureServer(t *testing.T) (*httptest.Server, *[]byte) {
	t.Helper()
	var captured []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf := make([]byte, 1<<16)
		n, _ := r.Body.Read(buf)
		captured = append(captured[:0], buf[:n]...)
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(srv.Close)
	return srv, &captured
}

func sampleEvent() lineage.RunEvent {
	return lineage.RunEvent{
		RunID:      "run-1",
		JobName:    "my-pipeline.read",
		Namespace:  "test-ns",
		ProducerID: "https://github.com/higino/eulantir",
		OccurredAt: time.Now(),
	}
}

// ---------------------------------------------------------------------------
// NewRunID
// ---------------------------------------------------------------------------

// TestNewRunID_NonEmpty verifies that NewRunID returns a non-empty string.
func TestNewRunID_NonEmpty(t *testing.T) {
	id := lineage.NewRunID()
	if id == "" {
		t.Error("NewRunID returned empty string")
	}
}

// TestNewRunID_Unique verifies that two consecutive calls return different IDs.
func TestNewRunID_Unique(t *testing.T) {
	a := lineage.NewRunID()
	b := lineage.NewRunID()
	if a == b {
		t.Errorf("NewRunID returned duplicate IDs: %q", a)
	}
}

// TestNewRunID_UUIDFormat verifies the ID has the expected UUID v4 dashed format.
func TestNewRunID_UUIDFormat(t *testing.T) {
	id := lineage.NewRunID()
	parts := strings.Split(id, "-")
	if len(parts) != 5 {
		t.Errorf("expected 5 dash-separated parts, got %d: %q", len(parts), id)
	}
}

// ---------------------------------------------------------------------------
// NoopEmitter
// ---------------------------------------------------------------------------

// TestNoopEmitter_NeverErrors verifies all three methods always return nil.
func TestNoopEmitter_NeverErrors(t *testing.T) {
	e := lineage.NoopEmitter{}
	evt := sampleEvent()
	if err := e.EmitStart(context.Background(), evt); err != nil {
		t.Errorf("EmitStart: %v", err)
	}
	if err := e.EmitComplete(context.Background(), evt); err != nil {
		t.Errorf("EmitComplete: %v", err)
	}
	if err := e.EmitFail(context.Background(), evt); err != nil {
		t.Errorf("EmitFail: %v", err)
	}
}

// TestNoopEmitter_ImplementsEmitter checks that NoopEmitter satisfies the interface.
func TestNoopEmitter_ImplementsEmitter(t *testing.T) {
	var _ lineage.Emitter = lineage.NoopEmitter{}
}

// ---------------------------------------------------------------------------
// HTTPEmitter — happy paths
// ---------------------------------------------------------------------------

// TestHTTPEmitter_EmitStart_SetsEventType verifies that EmitStart sends
// eventType = "START" in the JSON body.
func TestHTTPEmitter_EmitStart_SetsEventType(t *testing.T) {
	srv, body := captureServer(t)
	e := lineage.NewHTTPEmitter(srv.URL, "test-ns")

	if err := e.EmitStart(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("EmitStart: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(*body, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if got["eventType"] != "START" {
		t.Errorf("expected eventType=START, got %q", got["eventType"])
	}
}

// TestHTTPEmitter_EmitComplete_SetsEventType verifies eventType = "COMPLETE".
func TestHTTPEmitter_EmitComplete_SetsEventType(t *testing.T) {
	srv, body := captureServer(t)
	e := lineage.NewHTTPEmitter(srv.URL, "test-ns")

	if err := e.EmitComplete(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("EmitComplete: %v", err)
	}

	var got map[string]any
	json.Unmarshal(*body, &got)
	if got["eventType"] != "COMPLETE" {
		t.Errorf("expected eventType=COMPLETE, got %q", got["eventType"])
	}
}

// TestHTTPEmitter_EmitFail_SetsEventType verifies eventType = "FAIL".
func TestHTTPEmitter_EmitFail_SetsEventType(t *testing.T) {
	srv, body := captureServer(t)
	e := lineage.NewHTTPEmitter(srv.URL, "test-ns")

	if err := e.EmitFail(context.Background(), sampleEvent()); err != nil {
		t.Fatalf("EmitFail: %v", err)
	}

	var got map[string]any
	json.Unmarshal(*body, &got)
	if got["eventType"] != "FAIL" {
		t.Errorf("expected eventType=FAIL, got %q", got["eventType"])
	}
}

// TestHTTPEmitter_PostsToCorrectPath verifies the request hits /api/v1/lineage.
func TestHTTPEmitter_PostsToCorrectPath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusCreated)
	}))
	t.Cleanup(srv.Close)

	e := lineage.NewHTTPEmitter(srv.URL, "ns")
	_ = e.EmitStart(context.Background(), sampleEvent())

	if gotPath != "/api/v1/lineage" {
		t.Errorf("expected path /api/v1/lineage, got %q", gotPath)
	}
}

// TestHTTPEmitter_SetsContentType verifies the Content-Type header.
func TestHTTPEmitter_SetsContentType(t *testing.T) {
	var gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	e := lineage.NewHTTPEmitter(srv.URL, "ns")
	_ = e.EmitStart(context.Background(), sampleEvent())

	if gotCT != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", gotCT)
	}
}

// TestHTTPEmitter_DefaultNamespace verifies that if the event has no namespace,
// the emitter's namespace is filled in.
func TestHTTPEmitter_DefaultNamespace(t *testing.T) {
	srv, body := captureServer(t)
	e := lineage.NewHTTPEmitter(srv.URL, "default-ns")

	evt := sampleEvent()
	evt.Namespace = "" // leave empty — should be filled by emitter
	_ = e.EmitStart(context.Background(), evt)

	var got map[string]any
	json.Unmarshal(*body, &got)
	if got["namespace"] != "default-ns" {
		t.Errorf("expected namespace='default-ns', got %q", got["namespace"])
	}
}

// TestHTTPEmitter_ErrorFieldInFail verifies that an Error field on the event
// is serialised in the JSON body.
func TestHTTPEmitter_ErrorFieldInFail(t *testing.T) {
	srv, body := captureServer(t)
	e := lineage.NewHTTPEmitter(srv.URL, "ns")

	evt := sampleEvent()
	evt.Error = "something went wrong"
	_ = e.EmitFail(context.Background(), evt)

	var got map[string]any
	json.Unmarshal(*body, &got)
	if got["error"] != "something went wrong" {
		t.Errorf("expected error field, got %q", got["error"])
	}
}

// TestHTTPEmitter_JobFacets verifies that JobFacets are serialised.
func TestHTTPEmitter_JobFacets(t *testing.T) {
	srv, body := captureServer(t)
	e := lineage.NewHTTPEmitter(srv.URL, "ns")

	evt := sampleEvent()
	evt.JobFacets = map[string]any{"recordsIn": float64(42)}
	_ = e.EmitComplete(context.Background(), evt)

	var got map[string]any
	json.Unmarshal(*body, &got)
	facets, ok := got["jobFacets"].(map[string]any)
	if !ok {
		t.Fatalf("jobFacets missing or wrong type: %T", got["jobFacets"])
	}
	if facets["recordsIn"] != float64(42) {
		t.Errorf("expected recordsIn=42, got %v", facets["recordsIn"])
	}
}

// ---------------------------------------------------------------------------
// HTTPEmitter — error paths
// ---------------------------------------------------------------------------

// TestHTTPEmitter_Non2xxStatus verifies that a non-2xx response is an error.
func TestHTTPEmitter_Non2xxStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	e := lineage.NewHTTPEmitter(srv.URL, "ns")
	err := e.EmitStart(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention 500, got: %v", err)
	}
}

// TestHTTPEmitter_NetworkError verifies a connection failure returns an error.
func TestHTTPEmitter_NetworkError(t *testing.T) {
	e := lineage.NewHTTPEmitter("http://127.0.0.1:1", "ns") // port 1 always refused
	err := e.EmitStart(context.Background(), sampleEvent())
	if err == nil {
		t.Fatal("expected error for refused connection, got nil")
	}
	if !strings.Contains(err.Error(), "send lineage event") {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestHTTPEmitter_ContextCancelled verifies a cancelled context propagates.
func TestHTTPEmitter_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	e := lineage.NewHTTPEmitter(srv.URL, "ns")
	err := e.EmitStart(ctx, sampleEvent())
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// ---------------------------------------------------------------------------
// Factory — New()
// ---------------------------------------------------------------------------

// TestNew_NoopWhenDisabled verifies that disabled lineage → NoopEmitter.
func TestNew_NoopWhenDisabled(t *testing.T) {
	e := lineage.New(config.LineageConfig{Enabled: false, Endpoint: "http://marquez"})
	if _, ok := e.(lineage.NoopEmitter); !ok {
		t.Errorf("expected NoopEmitter when disabled, got %T", e)
	}
}

// TestNew_NoopWhenNoEndpoint verifies that enabled but empty endpoint → Noop.
func TestNew_NoopWhenNoEndpoint(t *testing.T) {
	e := lineage.New(config.LineageConfig{Enabled: true, Endpoint: ""})
	if _, ok := e.(lineage.NoopEmitter); !ok {
		t.Errorf("expected NoopEmitter for empty endpoint, got %T", e)
	}
}

// TestNew_HTTPEmitterWhenEnabled verifies that a configured emitter is HTTP.
func TestNew_HTTPEmitterWhenEnabled(t *testing.T) {
	e := lineage.New(config.LineageConfig{
		Enabled:   true,
		Endpoint:  "http://marquez:5000",
		Namespace: "my-ns",
	})
	if _, ok := e.(*lineage.HTTPEmitter); !ok {
		t.Errorf("expected *HTTPEmitter, got %T", e)
	}
}

// TestNew_DefaultNamespace verifies the namespace defaults to "eulantir".
func TestNew_DefaultNamespace(t *testing.T) {
	srv, body := captureServer(t)
	e := lineage.New(config.LineageConfig{
		Enabled:  true,
		Endpoint: srv.URL,
		// Namespace left empty → should default to "eulantir"
	})
	evt := sampleEvent()
	evt.Namespace = ""
	_ = e.EmitStart(context.Background(), evt)

	var got map[string]any
	json.Unmarshal(*body, &got)
	if got["namespace"] != "eulantir" {
		t.Errorf("expected default namespace 'eulantir', got %q", got["namespace"])
	}
}
