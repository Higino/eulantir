package agent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/llm"
)

const maxRetries = 3

// GeneratedPipeline holds everything the LLM produced for a single intent.
type GeneratedPipeline struct {
	PipelineYAML string
	GoFiles      map[string]string // filename (e.g. "filter_active.go") → Go source
}

// Agent generates pipeline configs and transform code from plain-English intents.
type Agent struct {
	provider llm.Provider
	model    string
	catalog  []connector.ConnectorInfo // live connector catalog for the system prompt
}

// New creates an Agent backed by the given provider, model, and connector catalog.
// Pass connector.Default.List() as catalog to use all registered connectors.
func New(provider llm.Provider, model string, catalog []connector.ConnectorInfo) *Agent {
	return &Agent{provider: provider, model: model, catalog: catalog}
}

// Generate takes a plain-English intent and returns a GeneratedPipeline.
// It retries up to maxRetries times, feeding parse and validation errors back
// to the LLM as correction requests.
func (a *Agent) Generate(ctx context.Context, intent string) (*GeneratedPipeline, error) {
	msgs := []llm.Message{
		{Role: llm.RoleSystem, Content: SystemPrompt(a.catalog)},
		{Role: llm.RoleUser, Content: intent},
	}

	for attempt := 1; attempt <= maxRetries; attempt++ {
		slog.Info("calling LLM", "attempt", attempt, "provider", a.provider.Name(), "model", a.model)

		resp, err := a.provider.Complete(ctx, llm.CompletionRequest{
			Model:       a.model,
			Messages:    msgs,
			Temperature: 0.2, // low temperature for deterministic structured output
		})
		if err != nil {
			return nil, fmt.Errorf("LLM request failed: %w", err)
		}

		slog.Debug("LLM response received",
			"input_tokens", resp.InputTokens,
			"output_tokens", resp.OutputTokens)

		// --- step 1: parse fenced blocks ---
		parsed, parseErr := Parse(resp.Content)
		if parseErr != nil {
			slog.Warn("parse failed, retrying", "attempt", attempt, "error", parseErr)
			msgs = append(msgs,
				llm.Message{Role: llm.RoleAssistant, Content: resp.Content},
				llm.Message{Role: llm.RoleUser, Content: fmt.Sprintf(
					"Your output could not be parsed. Error: %v\n\n"+
						"Remember the strict output format rules and try again.",
					parseErr,
				)},
			)
			continue
		}

		// --- step 2: validate the YAML structure ---
		if _, cfgErr := config.LoadBytes([]byte(parsed.PipelineYAML)); cfgErr != nil {
			slog.Warn("generated YAML failed validation, retrying",
				"attempt", attempt, "error", cfgErr)
			msgs = append(msgs,
				llm.Message{Role: llm.RoleAssistant, Content: resp.Content},
				llm.Message{Role: llm.RoleUser, Content: fmt.Sprintf(
					"Your pipeline YAML failed structural validation: %v\n\n"+
						"Fix the YAML so it passes validation and try again.",
					cfgErr,
				)},
			)
			continue
		}

		return &GeneratedPipeline{
			PipelineYAML: parsed.PipelineYAML,
			GoFiles:      parsed.GoFiles,
		}, nil
	}

	return nil, fmt.Errorf("agent failed to produce valid output after %d attempts", maxRetries)
}
