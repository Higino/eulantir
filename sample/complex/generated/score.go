// transform: score
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/higino/eulantir/internal/connector"
)

// Score computes a customer score from balance and status, drops inactive records.
// Sleeps 20 ms per record so the dashboard update is visible.
func Score(_ context.Context, in connector.Record) ([]connector.Record, error) {
	time.Sleep(20 * time.Millisecond)

	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("score: %w", err)
	}

	// Drop inactive customers.
	if row["status"] == "inactive" {
		return nil, nil
	}

	score := 0.0
	if bal, err := toFloat(row["balance"]); err == nil {
		score += bal * 0.01
	}
	switch row["status"] {
	case "premium":
		score += 50
	case "active":
		score += 20
	case "trial":
		score += 5
	}

	row["score"] = fmt.Sprintf("%.2f", score)
	row["tier"] = tier(score)

	out, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	in.Value = out
	return []connector.Record{in}, nil
}

func toFloat(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case string:
		return strconv.ParseFloat(x, 64)
	}
	return 0, fmt.Errorf("not a number")
}

func tier(score float64) string {
	switch {
	case score >= 100:
		return "gold"
	case score >= 50:
		return "silver"
	default:
		return "bronze"
	}
}
