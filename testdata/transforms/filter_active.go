// transform: filter_active
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/higino/eulantir/internal/connector"
)

// FilterActive drops records where the "status" field is not "active".
// Filter: returns nil, nil to drop; returns the original record to keep.
func FilterActive(ctx context.Context, in connector.Record) ([]connector.Record, error) {
	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("FilterActive: unmarshal: %w", err)
	}
	if row["status"] != "active" {
		return nil, nil // drop
	}
	return []connector.Record{in}, nil
}
