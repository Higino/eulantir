// transform: uppercase_name
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/higino/eulantir/internal/connector"
)

// UppercaseName rewrites the "name" field to uppercase.
// Map: always returns exactly one record (the modified input).
func UppercaseName(ctx context.Context, in connector.Record) ([]connector.Record, error) {
	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("UppercaseName: unmarshal: %w", err)
	}
	if name, ok := row["name"].(string); ok {
		row["name"] = strings.ToUpper(name)
	}
	modified, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("UppercaseName: marshal: %w", err)
	}
	out := in
	out.Value = modified
	return []connector.Record{out}, nil
}
