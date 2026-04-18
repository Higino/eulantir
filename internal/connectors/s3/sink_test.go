package s3

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"strings"
	"testing"

	"github.com/higino/eulantir/internal/connector"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func makeRecord(t *testing.T, m map[string]any) connector.Record {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("makeRecord: %v", err)
	}
	return connector.Record{Value: b}
}

// ---------------------------------------------------------------------------
// Configuration validation
// ---------------------------------------------------------------------------

func TestSink_Name(t *testing.T) {
	if (&Sink{}).Name() != "s3-sink" {
		t.Error("expected Name()='s3-sink'")
	}
}

func TestSink_Open_MissingBucket(t *testing.T) {
	err := (&Sink{}).Open(context.Background(), map[string]any{"key": "out.ndjson"})
	if err == nil {
		t.Fatal("expected error for missing bucket, got nil")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("error should mention 'bucket', got: %v", err)
	}
}

func TestSink_Open_MissingKey(t *testing.T) {
	err := (&Sink{}).Open(context.Background(), map[string]any{"bucket": "my-bucket"})
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
	if !strings.Contains(err.Error(), `"key"`) {
		t.Errorf("error should mention 'key', got: %v", err)
	}
}

func TestSink_Close_EmptyBuffer_NoUpload(t *testing.T) {
	// Close on an empty sink must be a no-op (no AWS call).
	s := &Sink{bucket: "b", key: "k", format: "ndjson"}
	if err := s.Close(context.Background()); err != nil {
		t.Errorf("Close on empty sink returned error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// NDJSON buffering
// ---------------------------------------------------------------------------

func TestSink_WriteNDJSON_BuffersLines(t *testing.T) {
	s := &Sink{format: "ndjson"}
	records := []connector.Record{
		makeRecord(t, map[string]any{"id": "1", "name": "Alice"}),
		makeRecord(t, map[string]any{"id": "2", "name": "Bob"}),
	}
	if err := s.WriteBatch(context.Background(), records); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}

	lines := strings.Split(strings.TrimRight(s.buf.String(), "\n"), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	var row map[string]any
	json.Unmarshal([]byte(lines[0]), &row)
	if row["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", row["name"])
	}
}

func TestSink_WriteNDJSON_DefaultFormat(t *testing.T) {
	// When format is empty, Open should default to ndjson.
	s := &Sink{}
	s.Open(context.Background(), map[string]any{"bucket": "b", "key": "k"}) //nolint
	if s.format != "ndjson" {
		t.Errorf("expected default format 'ndjson', got %q", s.format)
	}
}

func TestSink_WriteNDJSON_MultipleBatches(t *testing.T) {
	s := &Sink{format: "ndjson"}
	for i := range 3 {
		_ = i
		s.WriteBatch(context.Background(), []connector.Record{ //nolint
			makeRecord(t, map[string]any{"x": i}),
		})
	}
	lines := strings.Split(strings.TrimRight(s.buf.String(), "\n"), "\n")
	if len(lines) != 3 {
		t.Errorf("expected 3 lines across batches, got %d", len(lines))
	}
}

func TestSink_WriteNDJSON_EmptyBatch(t *testing.T) {
	s := &Sink{format: "ndjson"}
	if err := s.WriteBatch(context.Background(), nil); err != nil {
		t.Errorf("empty batch should be a no-op, got: %v", err)
	}
	if s.buf.Len() != 0 {
		t.Error("buffer should remain empty after empty batch")
	}
}

// ---------------------------------------------------------------------------
// CSV buffering
// ---------------------------------------------------------------------------

func TestSink_WriteCSV_BuffersHeaderAndRows(t *testing.T) {
	s := &Sink{format: "csv"}
	s.buf = bytes.Buffer{}
	s.csvW = csv.NewWriter(&s.buf)

	records := []connector.Record{
		makeRecord(t, map[string]any{"id": "1", "city": "NYC"}),
		makeRecord(t, map[string]any{"id": "2", "city": "LON"}),
	}
	if err := s.WriteBatch(context.Background(), records); err != nil {
		t.Fatalf("WriteBatch CSV: %v", err)
	}

	r := csv.NewReader(strings.NewReader(s.buf.String()))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse CSV buffer: %v", err)
	}
	if len(rows) != 3 { // header + 2 data rows
		t.Fatalf("expected 3 CSV rows (1 header + 2 data), got %d", len(rows))
	}
	if rows[0][0] != "city" || rows[0][1] != "id" {
		t.Errorf("headers should be alphabetically sorted: got %v", rows[0])
	}
}

func TestSink_WriteCSV_HeaderFixedAfterFirstBatch(t *testing.T) {
	s := &Sink{format: "csv"}
	s.buf = bytes.Buffer{}
	s.csvW = csv.NewWriter(&s.buf)

	// First batch — establishes headers
	s.WriteBatch(context.Background(), []connector.Record{makeRecord(t, map[string]any{"a": "1", "b": "2"})}) //nolint
	// Second batch — must reuse same header order
	s.WriteBatch(context.Background(), []connector.Record{makeRecord(t, map[string]any{"b": "4", "a": "3"})}) //nolint

	r := csv.NewReader(strings.NewReader(s.buf.String()))
	rows, _ := r.ReadAll()
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// header appears once
	if rows[0][0] != "a" {
		t.Errorf("expected header row[0][0]='a', got %q", rows[0][0])
	}
}

func TestSink_WriteCSV_MalformedRecord(t *testing.T) {
	s := &Sink{format: "csv"}
	s.buf = bytes.Buffer{}
	s.csvW = csv.NewWriter(&s.buf)

	err := s.WriteBatch(context.Background(), []connector.Record{{Value: []byte("not json")}})
	if err == nil {
		t.Error("expected error for malformed JSON record")
	}
}
