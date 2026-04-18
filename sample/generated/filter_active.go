// transform: filter_active
package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/higino/eulantir/internal/connector"
)

// FilterActive drops records whose "status" field is not "active".
func FilterActive(_ context.Context, in connector.Record) ([]connector.Record, error) {
	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("FilterActive: unmarshal: %w", err)
	}
	if row["status"] != "active" {
		return nil, nil
	}
	return []connector.Record{in}, nil
}
