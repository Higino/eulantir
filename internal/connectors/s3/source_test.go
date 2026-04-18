package s3

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// helpers for direct unit testing (no AWS credentials / network needed)
// ---------------------------------------------------------------------------

func newScanner(r io.Reader) *bufio.Scanner {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1<<20), 10<<20)
	return sc
}

func newCSVReader(r io.Reader) *csv.Reader {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true
	return cr
}

// ---------------------------------------------------------------------------
// Configuration validation tests (no AWS credentials needed)
// ---------------------------------------------------------------------------

func TestSource_Name(t *testing.T) {
	if (&Source{}).Name() != "s3" {
		t.Error("expected Name()='s3'")
	}
}

func TestSource_Open_MissingBucket(t *testing.T) {
	err := (&Source{}).Open(context.Background(), map[string]any{"key": "data.csv"})
	if err == nil {
		t.Fatal("expected error for missing bucket, got nil")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("error should mention 'bucket', got: %v", err)
	}
}

func TestSource_Open_MissingKey(t *testing.T) {
	err := (&Source{}).Open(context.Background(), map[string]any{"bucket": "my-bucket"})
	if err == nil {
		t.Fatal("expected error for missing key, got nil")
	}
	if !strings.Contains(err.Error(), `"key"`) {
		t.Errorf("error should mention key, got: %v", err)
	}
}

func TestSource_Close_Uninitialised(t *testing.T) {
	if err := (&Source{}).Close(context.Background()); err != nil {
		t.Errorf("Close on uninitialised source: %v", err)
	}
}

func TestSource_Commit_IsNoop(t *testing.T) {
	if err := (&Source{}).Commit(context.Background(), 99); err != nil {
		t.Errorf("Commit returned unexpected error: %v", err)
	}
}

func TestStringVal_S3(t *testing.T) {
	cfg := map[string]any{"region": "eu-west-1"}
	if got := stringVal(cfg, "region", "us-east-1"); got != "eu-west-1" {
		t.Errorf("expected 'eu-west-1', got %q", got)
	}
	if got := stringVal(cfg, "missing", "default"); got != "default" {
		t.Errorf("expected 'default', got %q", got)
	}
}

// ---------------------------------------------------------------------------
// NDJSON parsing — unit tested directly via scanner field
// ---------------------------------------------------------------------------

func TestReadNDJSON_ReturnsRecords(t *testing.T) {
	data := `{"id":"1","name":"Alice"}` + "\n" + `{"id":"2","name":"Bob"}` + "\n"
	s := &Source{format: "ndjson", scanner: newScanner(strings.NewReader(data))}

	batch, err := s.ReadBatch(context.Background(), 10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 records, got %d", len(batch))
	}
	var row map[string]any
	json.Unmarshal(batch[0].Value, &row)
	if row["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %v", row["name"])
	}
}

func TestReadNDJSON_EOF(t *testing.T) {
	s := &Source{format: "ndjson", scanner: newScanner(strings.NewReader(""))}
	_, err := s.ReadBatch(context.Background(), 10)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestReadNDJSON_SkipsBlankLines(t *testing.T) {
	data := "\n\n" + `{"id":"1"}` + "\n\n"
	s := &Source{format: "ndjson", scanner: newScanner(strings.NewReader(data))}
	batch, err := s.ReadBatch(context.Background(), 10)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch) != 1 {
		t.Errorf("expected 1 record (blank lines skipped), got %d", len(batch))
	}
}

func TestReadNDJSON_BatchSizeLimit(t *testing.T) {
	var sb strings.Builder
	for i := range 10 {
		fmt.Fprintf(&sb, `{"i":%d}`+"\n", i)
	}
	s := &Source{format: "ndjson", scanner: newScanner(strings.NewReader(sb.String()))}
	batch, err := s.ReadBatch(context.Background(), 3)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch) != 3 {
		t.Errorf("expected 3 records, got %d", len(batch))
	}
}

func TestReadNDJSON_OffsetIncremented(t *testing.T) {
	data := `{"x":"a"}` + "\n" + `{"x":"b"}` + "\n"
	s := &Source{format: "ndjson", scanner: newScanner(strings.NewReader(data))}
	batch, _ := s.ReadBatch(context.Background(), 10)
	if batch[0].Offset != 1 || batch[1].Offset != 2 {
		t.Errorf("expected offsets 1,2 — got %d,%d", batch[0].Offset, batch[1].Offset)
	}
}

// ---------------------------------------------------------------------------
// CSV parsing — unit tested directly via csvReader field
// ---------------------------------------------------------------------------

func TestReadCSV_ReturnsRecords(t *testing.T) {
	data := "id,name\n1,Alice\n2,Bob\n"
	cr := newCSVReader(strings.NewReader(data))
	hdrs, _ := cr.Read()
	s := &Source{format: "csv", csvReader: cr, headers: hdrs}

	batch, err := s.ReadBatch(context.Background(), 10)
	if err != nil && err != io.EOF {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(batch) != 2 {
		t.Fatalf("expected 2 records, got %d", len(batch))
	}
	var row map[string]string
	json.Unmarshal(batch[0].Value, &row)
	if row["name"] != "Alice" {
		t.Errorf("expected name=Alice, got %q", row["name"])
	}
}

func TestReadCSV_EOF(t *testing.T) {
	cr := newCSVReader(strings.NewReader(""))
	s := &Source{format: "csv", csvReader: cr, headers: []string{"id"}}
	_, err := s.ReadBatch(context.Background(), 10)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestReadCSV_BatchSizeLimit(t *testing.T) {
	var sb strings.Builder
	sb.WriteString("id\n")
	for i := range 10 {
		fmt.Fprintf(&sb, "%d\n", i)
	}
	cr := newCSVReader(strings.NewReader(sb.String()))
	hdrs, _ := cr.Read()
	s := &Source{format: "csv", csvReader: cr, headers: hdrs}

	batch, err := s.ReadBatch(context.Background(), 4)
	if err != nil && err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch) != 4 {
		t.Errorf("expected 4 records, got %d", len(batch))
	}
}

func TestReadCSV_OffsetIncremented(t *testing.T) {
	data := "col\na\nb\n"
	cr := newCSVReader(strings.NewReader(data))
	hdrs, _ := cr.Read()
	s := &Source{format: "csv", csvReader: cr, headers: hdrs}
	batch, _ := s.ReadBatch(context.Background(), 10)
	if batch[0].Offset != 1 || batch[1].Offset != 2 {
		t.Errorf("expected offsets 1,2 — got %d,%d", batch[0].Offset, batch[1].Offset)
	}
}
