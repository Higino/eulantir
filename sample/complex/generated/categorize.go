// transform: categorize
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/higino/eulantir/internal/connector"
)

// Categorize assigns a business category based on type and description.
// Sleeps 20 ms per record so the dashboard update is visible.
func Categorize(_ context.Context, in connector.Record) ([]connector.Record, error) {
	time.Sleep(20 * time.Millisecond)

	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("categorize: %w", err)
	}

	txType, _ := row["type"].(string)
	desc, _ := row["description"].(string)

	row["category"] = classify(txType, desc)

	out, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	in.Value = out
	return []connector.Record{in}, nil
}

func classify(txType, desc string) string {
	desc = strings.ToLower(desc)
	switch txType {
	case "refund":
		return "returns"
	case "deposit", "transfer":
		if strings.Contains(desc, "payroll") || strings.Contains(desc, "wire") {
			return "income"
		}
		return "transfers"
	case "withdrawal":
		return "cash"
	case "purchase":
		switch {
		case strings.Contains(desc, "subscription"):
			return "subscriptions"
		case strings.Contains(desc, "online"):
			return "e-commerce"
		default:
			return "retail"
		}
	}
	return "other"
}
