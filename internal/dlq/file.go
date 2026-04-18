package dlq

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/higino/eulantir/internal/connector"
)

// dlqEntry is the JSON structure written for each failed record.
type dlqEntry struct {
	NodeID    string            `json:"node_id"`
	Offset    int64             `json:"offset"`
	Key       []byte            `json:"key,omitempty"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Reason    string            `json:"reason"`
	FailedAt  time.Time         `json:"failed_at"`
}

// FileDLQ writes failed records to newline-delimited JSON files under a base
// directory. Each node gets its own subdirectory; files are named by date.
//
// Layout: <baseDir>/<nodeID>/<YYYY-MM-DD>.jsonl
type FileDLQ struct {
	baseDir string
}

// NewFileDLQ creates a FileDLQ that writes under baseDir.
func NewFileDLQ(baseDir string) *FileDLQ {
	return &FileDLQ{baseDir: baseDir}
}

func (d *FileDLQ) Push(_ context.Context, nodeID string, r connector.Record, reason error) error {
	dir := filepath.Join(d.baseDir, nodeID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("dlq: mkdir %s: %w", dir, err)
	}

	filename := filepath.Join(dir, time.Now().Format("2006-01-02")+".jsonl")
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("dlq: open %s: %w", filename, err)
	}
	defer f.Close()

	entry := dlqEntry{
		NodeID:   nodeID,
		Offset:   r.Offset,
		Key:      r.Key,
		Value:    r.Value,
		Headers:  r.Headers,
		Reason:   reason.Error(),
		FailedAt: time.Now().UTC(),
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("dlq: marshal entry: %w", err)
	}

	_, err = fmt.Fprintf(f, "%s\n", line)
	return err
}

func (d *FileDLQ) Drain(_ context.Context, nodeID string) (<-chan connector.Record, error) {
	ch := make(chan connector.Record)

	go func() {
		defer close(ch)

		pattern := filepath.Join(d.baseDir, nodeID, "*.jsonl")
		files, _ := filepath.Glob(pattern)

		for _, path := range files {
			f, err := os.Open(path)
			if err != nil {
				continue
			}
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				var entry dlqEntry
				if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
					continue
				}
				ch <- connector.Record{
					Key:     entry.Key,
					Value:   entry.Value,
					Headers: entry.Headers,
					Offset:  entry.Offset,
				}
			}
			f.Close()
		}
	}()

	return ch, nil
}

func (d *FileDLQ) Count(_ context.Context, nodeID string) (int64, error) {
	pattern := filepath.Join(d.baseDir, nodeID, "*.jsonl")
	files, _ := filepath.Glob(pattern)

	var count int64
	for _, path := range files {
		f, err := os.Open(path)
		if err != nil {
			continue
		}
		r := bufio.NewReader(f)
		for {
			_, err := r.ReadString('\n')
			if err == io.EOF {
				break
			}
			if err != nil {
				break
			}
			count++
		}
		f.Close()
	}
	return count, nil
}
