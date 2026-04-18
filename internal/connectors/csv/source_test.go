package csv

import (
	"context"
	"io"
	"os"
	"testing"
)

func writeTempCSV(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.csv")
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString(content)
	f.Close()
	return f.Name()
}

func TestSource_ReadAll(t *testing.T) {
	path := writeTempCSV(t, "id,name,status\n1,Alice,active\n2,Bob,inactive\n3,Carol,active\n")

	s := &Source{}
	if err := s.Open(context.Background(), map[string]any{"path": path}); err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	batch, err := s.ReadBatch(context.Background(), 100)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(batch) != 3 {
		t.Errorf("expected 3 records, got %d", len(batch))
	}

	// next read should return EOF
	_, err = s.ReadBatch(context.Background(), 100)
	if err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}

func TestSource_BatchSize(t *testing.T) {
	path := writeTempCSV(t, "id\n1\n2\n3\n4\n5\n")

	s := &Source{}
	s.Open(context.Background(), map[string]any{"path": path})
	defer s.Close(context.Background())

	b1, err := s.ReadBatch(context.Background(), 2)
	if err != nil || len(b1) != 2 {
		t.Errorf("expected 2 records in first batch, got %d (err=%v)", len(b1), err)
	}
	b2, err := s.ReadBatch(context.Background(), 2)
	if err != nil || len(b2) != 2 {
		t.Errorf("expected 2 records in second batch, got %d (err=%v)", len(b2), err)
	}
	b3, err := s.ReadBatch(context.Background(), 2)
	if err != nil || len(b3) != 1 {
		t.Errorf("expected 1 record in third batch, got %d (err=%v)", len(b3), err)
	}
}

func TestSource_MissingPath(t *testing.T) {
	s := &Source{}
	if err := s.Open(context.Background(), map[string]any{}); err == nil {
		t.Fatal("expected error for missing path")
	}
}

func TestSource_NoHeader(t *testing.T) {
	path := writeTempCSV(t, "a,b\nc,d\n")

	s := &Source{}
	s.Open(context.Background(), map[string]any{"path": path, "has_header": false})
	defer s.Close(context.Background())

	batch, _ := s.ReadBatch(context.Background(), 10)
	if len(batch) != 2 {
		t.Errorf("expected 2 records, got %d", len(batch))
	}
}
