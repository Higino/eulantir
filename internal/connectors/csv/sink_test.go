package csv

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/higino/eulantir/internal/connector"
)

// makeRecord encodes a map as a connector.Record whose Value is JSON.
func makeRecord(t *testing.T, fields map[string]any) connector.Record {
	t.Helper()
	b, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("makeRecord: marshal: %v", err)
	}
	return connector.Record{Value: b}
}

// readFile is a test helper that reads a file into a string.
func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("readFile %s: %v", path, err)
	}
	return string(b)
}

// TestSink_WriteAndRead writes a batch then reads the file back and checks
// that the headers and all values are present.
func TestSink_WriteAndRead(t *testing.T) {
	path := t.TempDir() + "/out.csv"

	s := &Sink{}
	if err := s.Open(context.Background(), map[string]any{"path": path}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	records := []connector.Record{
		makeRecord(t, map[string]any{"id": "1", "name": "Alice", "status": "active"}),
		makeRecord(t, map[string]any{"id": "2", "name": "Bob", "status": "inactive"}),
		makeRecord(t, map[string]any{"id": "3", "name": "Carol", "status": "active"}),
	}

	if err := s.WriteBatch(context.Background(), records); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := s.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	content := readFile(t, path)
	lines := strings.Split(strings.TrimSpace(content), "\n")

	// 1 header row + 3 data rows
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines (1 header + 3 rows), got %d:\n%s", len(lines), content)
	}

	// headers are sorted alphabetically
	if lines[0] != "id,name,status" {
		t.Errorf("expected header 'id,name,status', got %q", lines[0])
	}

	// spot-check first and last data rows
	if !strings.Contains(lines[1], "Alice") {
		t.Errorf("expected Alice in row 1, got %q", lines[1])
	}
	if !strings.Contains(lines[3], "Carol") {
		t.Errorf("expected Carol in row 3, got %q", lines[3])
	}
}

// TestSink_HeadersSortedAlphabetically verifies columns are always written in
// alphabetical order regardless of JSON key insertion order.
func TestSink_HeadersSortedAlphabetically(t *testing.T) {
	path := t.TempDir() + "/out.csv"

	s := &Sink{}
	s.Open(context.Background(), map[string]any{"path": path})

	// keys in reverse alpha order — headers must still come out sorted
	s.WriteBatch(context.Background(), []connector.Record{
		makeRecord(t, map[string]any{"zebra": "z", "apple": "a", "mango": "m"}),
	})
	s.Close(context.Background())

	content := readFile(t, path)
	header := strings.Split(strings.TrimSpace(content), "\n")[0]
	if header != "apple,mango,zebra" {
		t.Errorf("expected sorted header 'apple,mango,zebra', got %q", header)
	}
}

// TestSink_MultipleBatchesSameHeaders writes two batches and verifies the
// header is written only once (not duplicated on the second batch).
func TestSink_MultipleBatchesSameHeaders(t *testing.T) {
	path := t.TempDir() + "/out.csv"

	s := &Sink{}
	s.Open(context.Background(), map[string]any{"path": path})

	batch1 := []connector.Record{makeRecord(t, map[string]any{"id": "1", "val": "a"})}
	batch2 := []connector.Record{makeRecord(t, map[string]any{"id": "2", "val": "b"})}

	s.WriteBatch(context.Background(), batch1)
	s.WriteBatch(context.Background(), batch2)
	s.Close(context.Background())

	content := readFile(t, path)
	lines := strings.Split(strings.TrimSpace(content), "\n")

	// 1 header + 2 data rows — NOT 2 headers
	if len(lines) != 3 {
		t.Errorf("expected 3 lines (1 header + 2 rows), got %d:\n%s", len(lines), content)
	}
	if lines[0] != "id,val" {
		t.Errorf("expected header 'id,val', got %q", lines[0])
	}
}

// TestSink_EmptyBatch verifies that writing an empty batch produces no rows
// and does not write a header.
func TestSink_EmptyBatch(t *testing.T) {
	path := t.TempDir() + "/out.csv"

	s := &Sink{}
	s.Open(context.Background(), map[string]any{"path": path})
	s.WriteBatch(context.Background(), []connector.Record{})
	s.Close(context.Background())

	content := readFile(t, path)
	if strings.TrimSpace(content) != "" {
		t.Errorf("expected empty file after empty batch, got %q", content)
	}
}

// TestSink_MissingPath verifies that Open returns an error when no path is given.
func TestSink_MissingPath(t *testing.T) {
	s := &Sink{}
	if err := s.Open(context.Background(), map[string]any{}); err == nil {
		t.Fatal("expected error for missing path config key, got nil")
	}
}

// TestSink_InvalidPath verifies that Open returns an error for an
// unwritable/invalid path.
func TestSink_InvalidPath(t *testing.T) {
	s := &Sink{}
	err := s.Open(context.Background(), map[string]any{"path": "/no/such/directory/out.csv"})
	if err == nil {
		t.Fatal("expected error for invalid path, got nil")
	}
}

// TestSink_MalformedRecord verifies that WriteBatch returns an error when a
// record's Value is not valid JSON.
func TestSink_MalformedRecord(t *testing.T) {
	path := t.TempDir() + "/out.csv"

	s := &Sink{}
	s.Open(context.Background(), map[string]any{"path": path})

	bad := connector.Record{Value: []byte("not-json")}
	if err := s.WriteBatch(context.Background(), []connector.Record{bad}); err == nil {
		t.Fatal("expected error for malformed JSON record, got nil")
	}
}

// TestSink_NumericAndBoolValues verifies that non-string field values
// (numbers, booleans) are serialised correctly into CSV cells.
func TestSink_NumericAndBoolValues(t *testing.T) {
	path := t.TempDir() + "/out.csv"

	s := &Sink{}
	s.Open(context.Background(), map[string]any{"path": path})
	s.WriteBatch(context.Background(), []connector.Record{
		makeRecord(t, map[string]any{"active": true, "score": 42.5, "user": "Dave"}),
	})
	s.Close(context.Background())

	content := readFile(t, path)
	lines := strings.Split(strings.TrimSpace(content), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d", len(lines))
	}
	// headers: active,score,user (sorted)
	if lines[0] != "active,score,user" {
		t.Errorf("unexpected header: %q", lines[0])
	}
	// values row should contain the serialised forms
	if !strings.Contains(lines[1], "true") {
		t.Errorf("expected 'true' in data row, got %q", lines[1])
	}
	if !strings.Contains(lines[1], "42.5") {
		t.Errorf("expected '42.5' in data row, got %q", lines[1])
	}
}
