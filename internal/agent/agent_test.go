package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/higino/eulantir/internal/llm"
)

// ---------------------------------------------------------------------------
// mockProvider — controllable stub for llm.Provider
// ---------------------------------------------------------------------------

// mockProvider returns a pre-set sequence of responses and errors, cycling
// through them on successive Complete() calls.
type mockProvider struct {
	responses []llm.CompletionResponse
	errs      []error
	calls     int
}

func (m *mockProvider) Name() string { return "mock" }

func (m *mockProvider) Complete(_ context.Context, _ llm.CompletionRequest) (llm.CompletionResponse, error) {
	i := m.calls
	if i >= len(m.responses) {
		i = len(m.responses) - 1 // repeat last entry if sequence is exhausted
	}
	m.calls++
	if m.errs != nil && i < len(m.errs) && m.errs[i] != nil {
		return llm.CompletionResponse{}, m.errs[i]
	}
	return m.responses[i], nil
}

// ---------------------------------------------------------------------------
// Minimal valid YAML — passes config.LoadBytes
// ---------------------------------------------------------------------------

const minValidYAML = `version: "1"
name: test-pipeline
connectors:
  - name: src
    type: csv
    config:
      path: /tmp/test.csv
tasks:
  - id: read
    connector: src`

func validResponse(yaml string) llm.CompletionResponse {
	return llm.CompletionResponse{
		Content: fmt.Sprintf("```yaml\n%s\n```", yaml),
	}
}

// ---------------------------------------------------------------------------
// Generate — success paths
// ---------------------------------------------------------------------------

// TestAgent_Generate_SuccessFirstAttempt verifies that Generate succeeds
// when the provider returns valid output on the first call.
func TestAgent_Generate_SuccessFirstAttempt(t *testing.T) {
	p := &mockProvider{
		responses: []llm.CompletionResponse{validResponse(minValidYAML)},
	}
	a := New(p, "test-model", nil)

	out, err := a.Generate(context.Background(), "read csv and write to csv")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil GeneratedPipeline")
	}
	if !strings.Contains(out.PipelineYAML, "test-pipeline") {
		t.Errorf("unexpected YAML: %q", out.PipelineYAML)
	}
	if p.calls != 1 {
		t.Errorf("expected 1 LLM call, got %d", p.calls)
	}
}

// TestAgent_Generate_WithGoFile verifies that Go transform blocks are
// captured in the returned GeneratedPipeline.GoFiles map.
func TestAgent_Generate_WithGoFile(t *testing.T) {
	yaml := minValidYAML // base yaml, no transform — we just want YAML to validate
	content := fmt.Sprintf(
		"```yaml\n%s\n```\n\n```go\n// transform: filter_nulls\npackage main\nfunc F() {}\n```",
		yaml,
	)
	p := &mockProvider{
		responses: []llm.CompletionResponse{{Content: content}},
	}
	a := New(p, "test-model", nil)

	out, err := a.Generate(context.Background(), "filter nulls")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, ok := out.GoFiles["filter_nulls.go"]; !ok {
		t.Errorf("expected GoFiles[\"filter_nulls.go\"], got keys: %v", keys(out.GoFiles))
	}
}

// TestAgent_Generate_RetryAfterParseError verifies that a parse failure on
// the first attempt causes a retry, and the second good response is used.
func TestAgent_Generate_RetryAfterParseError(t *testing.T) {
	p := &mockProvider{
		responses: []llm.CompletionResponse{
			{Content: "oops — no code blocks here"},   // attempt 1: parse fails
			validResponse(minValidYAML),                // attempt 2: valid
		},
	}
	a := New(p, "test-model", nil)

	out, err := a.Generate(context.Background(), "test intent")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if out == nil {
		t.Fatal("expected non-nil output")
	}
	if p.calls != 2 {
		t.Errorf("expected 2 LLM calls (1 retry), got %d", p.calls)
	}
}

// TestAgent_Generate_RetryAfterYAMLValidationError verifies that a YAML
// structure error triggers a retry, and the corrected response succeeds.
func TestAgent_Generate_RetryAfterYAMLValidationError(t *testing.T) {
	// valid YAML block but missing required "name" field → LoadBytes rejects it
	badYAML := `version: "1"
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src`

	p := &mockProvider{
		responses: []llm.CompletionResponse{
			validResponse(badYAML),      // attempt 1: validation fails
			validResponse(minValidYAML), // attempt 2: valid
		},
	}
	a := New(p, "test-model", nil)

	out, err := a.Generate(context.Background(), "test intent")
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if p.calls != 2 {
		t.Errorf("expected 2 LLM calls, got %d", p.calls)
	}
	_ = out
}

// ---------------------------------------------------------------------------
// Generate — failure paths
// ---------------------------------------------------------------------------

// TestAgent_Generate_AllRetriesExhausted verifies that after maxRetries
// attempts with parse errors, Generate returns a non-nil error.
func TestAgent_Generate_AllRetriesExhausted(t *testing.T) {
	badResponse := llm.CompletionResponse{Content: "no yaml block here"}
	p := &mockProvider{
		responses: []llm.CompletionResponse{badResponse, badResponse, badResponse},
	}
	a := New(p, "test-model", nil)

	_, err := a.Generate(context.Background(), "test intent")
	if err == nil {
		t.Fatal("expected error after all retries exhausted, got nil")
	}
	if p.calls != maxRetries {
		t.Errorf("expected %d LLM calls, got %d", maxRetries, p.calls)
	}
}

// TestAgent_Generate_ProviderError verifies that an LLM request error is
// propagated immediately (no retries — it's a hard failure).
func TestAgent_Generate_ProviderError(t *testing.T) {
	p := &mockProvider{
		responses: []llm.CompletionResponse{{}},
		errs:      []error{errors.New("network unreachable")},
	}
	a := New(p, "test-model", nil)

	_, err := a.Generate(context.Background(), "test intent")
	if err == nil {
		t.Fatal("expected error for provider failure, got nil")
	}
	if !strings.Contains(err.Error(), "LLM request failed") {
		t.Errorf("unexpected error: %v", err)
	}
	if p.calls != 1 {
		t.Errorf("expected 1 call (no retry on hard error), got %d", p.calls)
	}
}

// TestAgent_Generate_ContextCancelled verifies that a cancelled context
// propagates as an error from the provider.
func TestAgent_Generate_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before calling

	// Provider that honours context cancellation
	p := &cancellingProvider{}
	a := New(p, "test-model", nil)

	_, err := a.Generate(ctx, "test intent")
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}

// cancellingProvider returns context.Canceled when ctx is done.
type cancellingProvider struct{}

func (c *cancellingProvider) Name() string { return "cancelling" }
func (c *cancellingProvider) Complete(ctx context.Context, _ llm.CompletionRequest) (llm.CompletionResponse, error) {
	select {
	case <-ctx.Done():
		return llm.CompletionResponse{}, ctx.Err()
	default:
		return llm.CompletionResponse{}, errors.New("unexpected call")
	}
}
