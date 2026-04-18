package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/higino/eulantir/internal/llm"
	"github.com/higino/eulantir/internal/profiler"
)

// FieldAnnotation is the LLM-generated semantic annotation for one field.
type FieldAnnotation struct {
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	SemanticType string   `json:"semantic_type"`
	BusinessTerm string   `json:"business_term"`
	PII          bool     `json:"pii"`
	Tags         []string `json:"tags,omitempty"`
}

// EnrichedProfile combines statistical profiling with LLM-generated annotations.
type EnrichedProfile struct {
	Profile            profiler.DatasetProfile `json:"profile"`
	DatasetDescription string                  `json:"dataset_description"`
	Domain             string                  `json:"domain"`
	Annotations        []FieldAnnotation       `json:"annotations"`
	EnrichedAt         time.Time               `json:"enriched_at"`
	Model              string                  `json:"model"`
}

// Enricher sends a DatasetProfile to an LLM and returns semantic annotations.
type Enricher struct {
	provider llm.Provider
	model    string
}

// New creates an Enricher backed by the given LLM provider.
func New(provider llm.Provider, model string) *Enricher {
	return &Enricher{provider: provider, model: model}
}

// Enrich sends the profile to the LLM and returns an EnrichedProfile.
func (e *Enricher) Enrich(ctx context.Context, p *profiler.DatasetProfile) (*EnrichedProfile, error) {
	prompt := buildPrompt(p)

	resp, err := e.provider.Complete(ctx, llm.CompletionRequest{
		Model:       e.model,
		MaxTokens:   4096,
		Temperature: 0.1,
		Messages: []llm.Message{
			{Role: llm.RoleSystem, Content: systemPrompt},
			{Role: llm.RoleUser, Content: prompt},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("LLM enrichment: %w", err)
	}

	ep, err := parseResponse(resp.Content, p)
	if err != nil {
		return nil, fmt.Errorf("parse LLM response: %w", err)
	}
	ep.EnrichedAt = time.Now().UTC()
	ep.Model = resp.Model
	if ep.Model == "" {
		ep.Model = e.model
	}
	return ep, nil
}

// ── prompting ──────────────────────────────────────────────────────────────

const systemPrompt = `You are a data catalog expert. Given a statistical profile of a dataset, you produce a JSON annotation describing the dataset and each of its fields.

You must respond with a single JSON object — no markdown fences, no extra text. The JSON must match this schema exactly:

{
  "dataset_description": "short description of what this dataset represents",
  "domain": "the business domain (e.g. finance, e-commerce, healthcare, logistics)",
  "annotations": [
    {
      "name": "field_name",
      "description": "what this field represents",
      "semantic_type": "one of: id, name, email, phone, address, date, timestamp, currency, percentage, category, flag, url, uuid, text, metric, other",
      "business_term": "the common business term for this field (e.g. 'customer identifier', 'transaction amount')",
      "pii": true or false,
      "tags": ["optional", "tags"]
    }
  ]
}`

func buildPrompt(p *profiler.DatasetProfile) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Dataset: %s\n", p.Source)
	fmt.Fprintf(&sb, "Records sampled: %d\n", p.TotalSeen)
	fmt.Fprintf(&sb, "Fields (%d):\n\n", len(p.Fields))

	for _, f := range p.Fields {
		fmt.Fprintf(&sb, "Field: %s\n", f.Name)
		fmt.Fprintf(&sb, "  Type: %s\n", f.Type)
		fmt.Fprintf(&sb, "  Null rate: %.1f%%\n", f.NullRate*100)
		fmt.Fprintf(&sb, "  Cardinality: %d distinct values\n", f.Cardinality)
		if f.Pattern != "" {
			fmt.Fprintf(&sb, "  Pattern detected: %s\n", f.Pattern)
		}
		if f.Min != nil && f.Max != nil && f.Mean != nil {
			fmt.Fprintf(&sb, "  Range: min=%.4g max=%.4g mean=%.4g\n", *f.Min, *f.Max, *f.Mean)
		}
		if f.MinLen != nil && f.MaxLen != nil {
			fmt.Fprintf(&sb, "  String length: %d–%d chars\n", *f.MinLen, *f.MaxLen)
		}
		if len(f.Examples) > 0 {
			fmt.Fprintf(&sb, "  Examples: %s\n", strings.Join(f.Examples, ", "))
		}
		sb.WriteByte('\n')
	}

	sb.WriteString("\nAnnotate this dataset. Respond only with the JSON object described in the system prompt.")
	return sb.String()
}

// llmResponse is the JSON structure we expect back from the LLM.
type llmResponse struct {
	DatasetDescription string            `json:"dataset_description"`
	Domain             string            `json:"domain"`
	Annotations        []FieldAnnotation `json:"annotations"`
}

func parseResponse(content string, p *profiler.DatasetProfile) (*EnrichedProfile, error) {
	// Strip any accidental markdown fences.
	content = strings.TrimSpace(content)
	if strings.HasPrefix(content, "```") {
		start := strings.Index(content, "\n")
		end := strings.LastIndex(content, "```")
		if start >= 0 && end > start {
			content = strings.TrimSpace(content[start+1 : end])
		}
	}

	var r llmResponse
	if err := json.Unmarshal([]byte(content), &r); err != nil {
		return nil, fmt.Errorf("invalid JSON from LLM: %w\nraw: %s", err, content)
	}

	return &EnrichedProfile{
		Profile:            *p,
		DatasetDescription: r.DatasetDescription,
		Domain:             r.Domain,
		Annotations:        r.Annotations,
	}, nil
}
