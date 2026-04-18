package csv

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/higino/eulantir/internal/connector"
)

// Source reads records from a CSV file.
// Each row is emitted as a connector.Record whose Value is a JSON-encoded
// map[string]string keyed by column header.
type Source struct {
	path      string
	delimiter rune
	hasHeader bool

	file    *os.File
	reader  *csv.Reader
	headers []string
	offset  int64
}

func (s *Source) Name() string { return "csv" }

func (s *Source) Open(_ context.Context, cfg map[string]any) error {
	s.path = stringVal(cfg, "path", "")
	if s.path == "" {
		return fmt.Errorf("csv source: missing required config key \"path\"")
	}
	delim := stringVal(cfg, "delimiter", ",")
	if len(delim) > 0 {
		s.delimiter = rune(delim[0])
	} else {
		s.delimiter = ','
	}
	s.hasHeader = boolVal(cfg, "has_header", true)

	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("csv source: open %s: %w", s.path, err)
	}
	s.file = f
	s.reader = csv.NewReader(f)
	s.reader.Comma = s.delimiter
	s.reader.TrimLeadingSpace = true

	if s.hasHeader {
		headers, err := s.reader.Read()
		if err != nil {
			return fmt.Errorf("csv source: read headers: %w", err)
		}
		s.headers = headers
	}
	return nil
}

func (s *Source) Close(_ context.Context) error {
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}

// ReadBatch reads up to maxSize rows and returns them as Records.
// Returns io.EOF when the file is exhausted.
func (s *Source) ReadBatch(_ context.Context, maxSize int) ([]connector.Record, error) {
	var records []connector.Record

	for len(records) < maxSize {
		row, err := s.reader.Read()
		if err == io.EOF {
			if len(records) == 0 {
				return nil, io.EOF
			}
			return records, nil
		}
		if err != nil {
			return records, fmt.Errorf("csv source: read row: %w", err)
		}

		rowMap := make(map[string]string, len(row))
		if len(s.headers) > 0 {
			for i, h := range s.headers {
				if i < len(row) {
					rowMap[h] = row[i]
				}
			}
		} else {
			for i, v := range row {
				rowMap[fmt.Sprintf("col%d", i)] = v
			}
		}

		val, err := json.Marshal(rowMap)
		if err != nil {
			return records, fmt.Errorf("csv source: marshal row: %w", err)
		}

		s.offset++
		records = append(records, connector.Record{
			Value:  val,
			Offset: s.offset,
		})
	}
	return records, nil
}

func (s *Source) Commit(_ context.Context, _ int64) error {
	// CSV files are read sequentially; no offset store needed.
	return nil
}

// helpers

func stringVal(cfg map[string]any, key, def string) string {
	if v, ok := cfg[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}

func boolVal(cfg map[string]any, key string, def bool) bool {
	if v, ok := cfg[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return def
}

func init() {
	connector.Default.RegisterSource(connector.ConnectorInfo{
		Type:        "csv",
		Description: "Read records from a local CSV file",
		ConfigKeys:  []string{"path", "delimiter", "has_header"},
	}, func(_ context.Context, cfg map[string]any) (connector.Source, error) {
		s := &Source{}
		if err := s.Open(context.Background(), cfg); err != nil {
			return nil, err
		}
		return s, nil
	})
}
