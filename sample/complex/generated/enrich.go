// transform: enrich
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/higino/eulantir/internal/connector"
)

// Enrich adds a human-readable amount_label and flags high-value transactions.
// Sleeps 20 ms per record so the dashboard update is visible.
func Enrich(_ context.Context, in connector.Record) ([]connector.Record, error) {
	time.Sleep(20 * time.Millisecond)

	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("enrich: %w", err)
	}

	amount, _ := parseFloat(row["amount"])
	absAmt := amount
	if absAmt < 0 {
		absAmt = -absAmt
	}

	switch {
	case absAmt >= 1000:
		row["amount_label"] = "large"
		row["high_value"] = true
	case absAmt >= 100:
		row["amount_label"] = "medium"
		row["high_value"] = false
	default:
		row["amount_label"] = "small"
		row["high_value"] = false
	}

	out, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	in.Value = out
	return []connector.Record{in}, nil
}

func parseFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return 0, fmt.Errorf("not a number")
}
