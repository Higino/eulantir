// transform: filter_low_value
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/higino/eulantir/internal/connector"
)

// FilterLowValue keeps orders with amount <= 100.
func FilterLowValue(_ context.Context, in connector.Record) ([]connector.Record, error) {
	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("FilterLowValue: unmarshal: %w", err)
	}
	amountStr, _ := row["amount"].(string)
	amount, err := strconv.ParseFloat(amountStr, 64)
	if err != nil || amount > 100 {
		return nil, nil
	}
	return []connector.Record{in}, nil
}
