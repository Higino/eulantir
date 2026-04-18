// transform: normalize
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/higino/eulantir/internal/connector"
)

// Normalize trims whitespace, title-cases names, lower-cases emails.
// Sleeps 20 ms per record so the dashboard update is visible.
func Normalize(_ context.Context, in connector.Record) ([]connector.Record, error) {
	time.Sleep(20 * time.Millisecond)

	var row map[string]any
	if err := json.Unmarshal(in.Value, &row); err != nil {
		return nil, fmt.Errorf("normalize: %w", err)
	}

	if name, ok := row["name"].(string); ok {
		row["name"] = titleCase(strings.TrimSpace(name))
	}
	if email, ok := row["email"].(string); ok {
		row["email"] = strings.ToLower(strings.TrimSpace(email))
	}
	row["_normalized"] = true

	out, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	in.Value = out
	return []connector.Record{in}, nil
}

func titleCase(s string) string {
	words := strings.Fields(s)
	for i, w := range words {
		runes := []rune(w)
		if len(runes) > 0 {
			runes[0] = unicode.ToUpper(runes[0])
		}
		words[i] = string(runes)
	}
	return strings.Join(words, " ")
}
