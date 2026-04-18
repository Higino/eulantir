package csv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/higino/eulantir/internal/connector"
)

// Sink writes records to a CSV file.
// Each record's Value must be a JSON-encoded map[string]string or map[string]any.
// Column order is sorted alphabetically on the first batch and held fixed.
type Sink struct {
	path    string
	file    *os.File
	writer  *csv.Writer
	headers []string
}

func (s *Sink) Name() string { return "csv-sink" }

func (s *Sink) Open(_ context.Context, cfg map[string]any) error {
	s.path = stringVal(cfg, "path", "")
	if s.path == "" {
		return fmt.Errorf("csv sink: missing required config key \"path\"")
	}
	f, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("csv sink: create %s: %w", s.path, err)
	}
	s.file = f
	s.writer = csv.NewWriter(f)
	return nil
}

func (s *Sink) Close(_ context.Context) error {
	if s.writer != nil {
		s.writer.Flush()
		if err := s.writer.Error(); err != nil {
			return err
		}
	}
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// WriteBatch writes a batch of records. Idempotency is not guaranteed for CSV
// (append-only); use the Postgres sink when idempotent writes are needed.
func (s *Sink) WriteBatch(_ context.Context, records []connector.Record) error {
	for _, r := range records {
		var row map[string]any
		if err := json.Unmarshal(r.Value, &row); err != nil {
			return fmt.Errorf("csv sink: unmarshal record: %w", err)
		}

		// on first record, derive and write headers
		if s.headers == nil {
			for k := range row {
				s.headers = append(s.headers, k)
			}
			sort.Strings(s.headers)
			if err := s.writer.Write(s.headers); err != nil {
				return fmt.Errorf("csv sink: write header: %w", err)
			}
		}

		vals := make([]string, len(s.headers))
		for i, h := range s.headers {
			vals[i] = fmt.Sprintf("%v", row[h])
		}
		if err := s.writer.Write(vals); err != nil {
			return fmt.Errorf("csv sink: write row: %w", err)
		}
	}
	s.writer.Flush()
	return s.writer.Error()
}

func init() {
	connector.Default.RegisterSink(connector.ConnectorInfo{
		Type:        "csv-sink",
		Description: "Write records to a local CSV file",
		ConfigKeys:  []string{"path"},
	}, func(_ context.Context, cfg map[string]any) (connector.Sink, error) {
		s := &Sink{}
		if err := s.Open(context.Background(), cfg); err != nil {
			return nil, err
		}
		return s, nil
	})
}
