// transform: validate
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/higino/eulantir/internal/connector"
)

// Validate drops transactions with missing or zero amounts.
// Sleeps 20 ms per record so the dashboard update is visible.
func Validate(_ context.Context, in connector.Record) ([]connector.Record, error) {
	time.Sleep(20 * time.Millisecond)

	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("validate: %w", err)
	}

	amount, err := toFloat64(row["amount"])
	if err != nil || amount == 0 {
		return nil, nil // drop invalid
	}

	row["amount_abs"] = fmt.Sprintf("%.2f", abs(amount))
	row["_valid"] = true

	out, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	in.Value = out
	return []connector.Record{in}, nil
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return 0, fmt.Errorf("not a number")
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
