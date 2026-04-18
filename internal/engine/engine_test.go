package engine

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/dag"
	"github.com/higino/eulantir/internal/dlq"

	_ "github.com/higino/eulantir/internal/connectors/csv"
)

// ---------------------------------------------------------------------------
// In-memory mock connectors — no files, no network, no Postgres
// ---------------------------------------------------------------------------

// mockSource returns predefined batches then io.EOF.
type mockSource struct {
	batches [][]connector.Record
	idx     int
}

func (m *mockSource) Name() string                                           { return "mock-src" }
func (m *mockSource) Open(_ context.Context, _ map[string]any) error        { return nil }
func (m *mockSource) Close(_ context.Context) error                         { return nil }
func (m *mockSource) Commit(_ context.Context, _ int64) error               { return nil }
func (m *mockSource) ReadBatch(_ context.Context, _ int) ([]connector.Record, error) {
	if m.idx >= len(m.batches) {
		return nil, io.EOF
	}
	b := m.batches[m.idx]
	m.idx++
	return b, nil
}

// mockSink accumulates written records. If errAfter > 0 it returns an error
// on every call after the first errAfter successful ones.
type mockSink struct {
	written   []connector.Record
	errAfter  int
	callCount int
}

func (m *mockSink) Name() string                                    { return "mock-sink" }
func (m *mockSink) Open(_ context.Context, _ map[string]any) error  { return nil }
func (m *mockSink) Close(_ context.Context) error                   { return nil }
func (m *mockSink) WriteBatch(_ context.Context, batch []connector.Record) error {
	m.callCount++
	// errAfter < 0  → always fail
	// errAfter > 0  → fail after the first errAfter successful calls
	// errAfter == 0 → never fail (default / no errors)
	if m.errAfter < 0 || (m.errAfter > 0 && m.callCount > m.errAfter) {
		return errors.New("mock write error")
	}
	m.written = append(m.written, batch...)
	return nil
}

// mockRegistry satisfies the connectorRegistry interface used by executor.
type mockRegistry struct {
	src connector.Source
	snk connector.Sink
}

func (r *mockRegistry) BuildSource(_ string, _ map[string]any) (connector.Source, error) {
	if r.src == nil {
		return nil, errors.New("no source registered")
	}
	return r.src, nil
}
func (r *mockRegistry) BuildSink(_ string, _ map[string]any) (connector.Sink, error) {
	if r.snk == nil {
		return nil, errors.New("no sink registered")
	}
	return r.snk, nil
}
func (r *mockRegistry) IsSink(_ string) bool { return r.snk != nil }

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func rec(t *testing.T, fields map[string]any) connector.Record {
	t.Helper()
	b, _ := json.Marshal(fields)
	return connector.Record{Value: b}
}

// minimalPipeline builds a PipelineConfig for a two-task (read → write)
// pipeline using the given connector types.
func minimalPipeline(srcType, sinkType string) config.PipelineConfig {
	return config.PipelineConfig{
		Version: "1",
		Name:    "engine-test",
		Connectors: []config.ConnectorConfig{
			{Name: "src", Type: srcType, Config: map[string]any{}},
			{Name: "dst", Type: sinkType, Config: map[string]any{}},
		},
		Tasks: []config.TaskConfig{
			{ID: "read", Connector: "src", DependsOn: []string{}},
			{ID: "write", Connector: "dst", DependsOn: []string{"read"}},
		},
	}
}

func collectResults(ch <-chan TaskResult) []TaskResult {
	var out []TaskResult
	for r := range ch {
		out = append(out, r)
	}
	return out
}

// ---------------------------------------------------------------------------
// executor unit tests
// ---------------------------------------------------------------------------

// TestExecutor_SourceNode verifies a source node reads all batches and
// reports the correct record counts.
func TestExecutor_SourceNode(t *testing.T) {
	src := &mockSource{batches: [][]connector.Record{
		{rec(t, map[string]any{"id": "1"}), rec(t, map[string]any{"id": "2"})},
		{rec(t, map[string]any{"id": "3"})},
	}}
	reg := &mockRegistry{src: src}

	node := dag.Node{ID: "read", ConnectorRef: "src"}
	cfg := minimalPipeline("mock-src", "mock-sink")

	outPipe := make(chan Batch, 10)
	exec := &executor{registry: reg, batchSize: 100}
	result := exec.run(context.Background(), node, cfg, nil, outPipe)

	if result.Status != StatusSuccess {
		t.Errorf("expected success, got %s (err: %v)", result.Status, result.Err)
	}
	if result.RecordsIn != 3 {
		t.Errorf("expected RecordsIn=3, got %d", result.RecordsIn)
	}
	if result.RecordsOut != 3 {
		t.Errorf("expected RecordsOut=3, got %d", result.RecordsOut)
	}
}

// TestExecutor_SinkNode verifies a sink node drains its inPipe and writes
// all records.
func TestExecutor_SinkNode(t *testing.T) {
	snk := &mockSink{}
	reg := &mockRegistry{snk: snk}

	node := dag.Node{ID: "write", ConnectorRef: "dst"}
	cfg := minimalPipeline("mock-src", "csv-sink") // "csv-sink" is recognised as a sink by resolveConnectors

	inPipe := make(chan Batch, 2)
	inPipe <- []connector.Record{rec(t, map[string]any{"id": "1"}), rec(t, map[string]any{"id": "2"})}
	inPipe <- []connector.Record{rec(t, map[string]any{"id": "3"})}
	close(inPipe)

	exec := &executor{registry: reg, batchSize: 100}
	result := exec.run(context.Background(), node, cfg, inPipe, nil)

	if result.Status != StatusSuccess {
		t.Errorf("expected success, got %s", result.Status)
	}
	if result.RecordsIn != 3 {
		t.Errorf("expected RecordsIn=3, got %d", result.RecordsIn)
	}
	if result.RecordsOut != 3 {
		t.Errorf("expected RecordsOut=3, got %d", result.RecordsOut)
	}
	if len(snk.written) != 3 {
		t.Errorf("expected 3 records written to sink, got %d", len(snk.written))
	}
}

// TestExecutor_SinkWriteError_DLQ verifies that when a sink write fails,
// records are pushed to the DLQ and RecordsDLQ is counted correctly.
func TestExecutor_SinkWriteError_DLQ(t *testing.T) {
	snk := &mockSink{errAfter: -1} // -1 = always fail
	reg := &mockRegistry{snk: snk}
	fileDLQ := dlq.NewFileDLQ(t.TempDir())

	node := dag.Node{ID: "write", ConnectorRef: "dst"}
	cfg := minimalPipeline("mock-src", "csv-sink") // "csv-sink" is recognised as a sink by resolveConnectors
	cfg.Retry = config.RetryConfig{MaxAttempts: 1} // no retries so test is fast

	inPipe := make(chan Batch, 1)
	inPipe <- []connector.Record{
		rec(t, map[string]any{"id": "1"}),
		rec(t, map[string]any{"id": "2"}),
	}
	close(inPipe)

	exec := &executor{registry: reg, dlq: fileDLQ, batchSize: 100,
		retryCfg: config.RetryConfig{MaxAttempts: 1}}
	result := exec.run(context.Background(), node, cfg, inPipe, nil)

	if result.Status != StatusSuccess {
		t.Errorf("expected success (DLQ swallows errors), got %s", result.Status)
	}
	if result.RecordsDLQ != 2 {
		t.Errorf("expected RecordsDLQ=2, got %d", result.RecordsDLQ)
	}

	count, err := fileDLQ.Count(context.Background(), "write")
	if err != nil {
		t.Fatalf("DLQ.Count: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 records in DLQ file, got %d", count)
	}
}

// TestExecutor_UnknownConnector verifies that a node referencing a connector
// name not in the config fails immediately.
func TestExecutor_UnknownConnector(t *testing.T) {
	reg := &mockRegistry{}
	node := dag.Node{ID: "read", ConnectorRef: "ghost"}
	cfg := minimalPipeline("mock-src", "mock-sink")

	outPipe := make(chan Batch, 1)
	exec := &executor{registry: reg, batchSize: 100}
	result := exec.run(context.Background(), node, cfg, nil, outPipe)

	if result.Status != StatusFailed {
		t.Errorf("expected failed, got %s", result.Status)
	}
	if result.Err == nil {
		t.Error("expected non-nil error for unknown connector")
	}
}

// TestExecutor_SkipNode verifies that a node with no connector and no
// transform ref is marked Skipped.
func TestExecutor_SkipNode(t *testing.T) {
	reg := &mockRegistry{}
	node := dag.Node{ID: "orphan"} // no ConnectorRef, no TransformRef

	inPipe := make(chan Batch)
	close(inPipe)
	exec := &executor{registry: reg, batchSize: 100}
	result := exec.run(context.Background(), node, config.PipelineConfig{}, inPipe, nil)

	if result.Status != StatusSkipped {
		t.Errorf("expected skipped, got %s", result.Status)
	}
}

// TestExecutor_ContextCancellation verifies that cancelling the context while
// a source is sending data causes the pipeline to stop with StatusFailed.
func TestExecutor_ContextCancellation(t *testing.T) {
	// source returns many batches — context will be cancelled before they're all read
	var batches [][]connector.Record
	for i := range 100 {
		batches = append(batches, []connector.Record{rec(t, map[string]any{"i": i})})
	}
	src := &mockSource{batches: batches}
	reg := &mockRegistry{src: src}

	node := dag.Node{ID: "read", ConnectorRef: "src"}
	cfg := minimalPipeline("mock-src", "mock-sink")

	ctx, cancel := context.WithCancel(context.Background())

	// outPipe with zero buffer — will block when the consumer is gone
	outPipe := make(chan Batch) // unbuffered: source will block trying to send
	cancel()                   // cancel immediately

	exec := &executor{registry: reg, batchSize: 1}
	result := exec.run(ctx, node, cfg, nil, outPipe)

	if result.Status != StatusFailed {
		t.Errorf("expected failed after context cancel, got %s", result.Status)
	}
}

// TestExecutor_FinishedAtAlwaysSet verifies the named-return defer correctly
// sets FinishedAt even on early error returns.
func TestExecutor_FinishedAtAlwaysSet(t *testing.T) {
	reg := &mockRegistry{} // no source or sink → BuildSource will fail
	node := dag.Node{ID: "read", ConnectorRef: "src"}
	cfg := minimalPipeline("mock-src", "mock-sink")

	exec := &executor{registry: reg, batchSize: 100}
	result := exec.run(context.Background(), node, cfg, nil, make(chan Batch, 1))

	if result.FinishedAt.IsZero() {
		t.Error("FinishedAt must never be zero — named-return defer not firing")
	}
	dur := result.FinishedAt.Sub(result.StartedAt)
	if dur < 0 {
		t.Errorf("FinishedAt is before StartedAt: duration=%s", dur)
	}
}

// ---------------------------------------------------------------------------
// LocalEngine integration tests (uses real CSV connectors + temp files)
// ---------------------------------------------------------------------------

// TestLocalEngine_CSVSourceToCSVSink is a full integration test:
// CSV source → LocalEngine → CSV sink, via the registered connector registry.
func TestLocalEngine_CSVSourceToCSVSink(t *testing.T) {
	// write a small input CSV
	inputPath := t.TempDir() + "/input.csv"
	if err := writeCSV(inputPath, "id,name\n1,Alice\n2,Bob\n3,Carol\n"); err != nil {
		t.Fatal(err)
	}
	outputPath := t.TempDir() + "/output.csv"

	cfg := config.PipelineConfig{
		Version: "1",
		Name:    "csv-to-csv",
		Connectors: []config.ConnectorConfig{
			{Name: "src", Type: "csv", Config: map[string]any{"path": inputPath}},
			{Name: "dst", Type: "csv-sink", Config: map[string]any{"path": outputPath}},
		},
		Tasks: []config.TaskConfig{
			{ID: "read", Connector: "src", DependsOn: []string{}},
			{ID: "write", Connector: "dst", DependsOn: []string{"read"}},
		},
	}

	eng := &LocalEngine{Registry: connector.Default}
	results, err := eng.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	var readResult, writeResult TaskResult
	for r := range results {
		switch r.NodeID {
		case "read":
			readResult = r
		case "write":
			writeResult = r
		}
	}

	if readResult.Status != StatusSuccess {
		t.Errorf("read: expected success, got %s (err: %v)", readResult.Status, readResult.Err)
	}
	if readResult.RecordsIn != 3 {
		t.Errorf("read: expected RecordsIn=3, got %d", readResult.RecordsIn)
	}
	if writeResult.Status != StatusSuccess {
		t.Errorf("write: expected success, got %s (err: %v)", writeResult.Status, writeResult.Err)
	}
	if writeResult.RecordsOut != 3 {
		t.Errorf("write: expected RecordsOut=3, got %d", writeResult.RecordsOut)
	}
}

// TestLocalEngine_EmptySource verifies a pipeline with no records completes
// successfully with zero counts.
func TestLocalEngine_EmptySource(t *testing.T) {
	src := &mockSource{batches: [][]connector.Record{}}
	snk := &mockSink{}

	reg := connector.NewRegistry()
	reg.RegisterSource(connector.ConnectorInfo{Type: "mock-src"}, func(_ context.Context, _ map[string]any) (connector.Source, error) {
		return src, nil
	})
	// Register under "csv-sink" so resolveConnectors treats it as a sink.
	reg.RegisterSink(connector.ConnectorInfo{Type: "csv-sink"}, func(_ context.Context, _ map[string]any) (connector.Sink, error) {
		return snk, nil
	})

	cfg := minimalPipeline("mock-src", "csv-sink")
	eng := &LocalEngine{Registry: reg}
	results, err := eng.Run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	for r := range results {
		if r.Status != StatusSuccess {
			t.Errorf("node %s: expected success, got %s", r.NodeID, r.Status)
		}
		if r.RecordsIn != 0 || r.RecordsOut != 0 {
			t.Errorf("node %s: expected zero counts, got in=%d out=%d",
				r.NodeID, r.RecordsIn, r.RecordsOut)
		}
	}
}

// TestLocalEngine_CycleDetected verifies that a cyclic DAG is rejected by Run.
func TestLocalEngine_CycleDetected(t *testing.T) {
	cfg := config.PipelineConfig{
		Version: "1",
		Name:    "cycle-test",
		Connectors: []config.ConnectorConfig{
			{Name: "src", Type: "mock-src"},
		},
		Tasks: []config.TaskConfig{
			{ID: "a", Connector: "src", DependsOn: []string{"b"}},
			{ID: "b", Connector: "src", DependsOn: []string{"a"}},
		},
	}
	eng := &LocalEngine{Registry: connector.NewRegistry()}
	_, err := eng.Run(context.Background(), cfg)
	if err == nil {
		t.Fatal("expected error for cyclic DAG, got nil")
	}
}

// ---------------------------------------------------------------------------
// retry.go coverage
// ---------------------------------------------------------------------------

// TestNewBackoff_Defaults verifies that newBackoff with zero-value config
// still returns a valid (non-nil) backoff.
func TestNewBackoff_Defaults(t *testing.T) {
	b := newBackoff(config.RetryConfig{})
	if b == nil {
		t.Error("newBackoff returned nil for zero-value config")
	}
}

// TestNewBackoff_CustomValues verifies that custom durations and multiplier
// are parsed and applied without panicking.
func TestNewBackoff_CustomValues(t *testing.T) {
	b := newBackoff(config.RetryConfig{
		MaxAttempts:     5,
		InitialInterval: "500ms",
		MaxInterval:     "10s",
		Multiplier:      3.0,
		Jitter:          true,
	})
	if b == nil {
		t.Error("newBackoff returned nil for custom config")
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func writeCSV(path, content string) error {
	return writeFile(path, []byte(content))
}

func writeFile(path string, data []byte) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write(data)
	return err
}
