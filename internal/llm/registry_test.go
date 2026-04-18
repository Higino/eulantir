package llm

import (
	"os"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// BuildProvider
// ---------------------------------------------------------------------------

// TestBuildProvider_OllamaDefault verifies that an empty type defaults to Ollama.
func TestBuildProvider_OllamaDefault(t *testing.T) {
	p, err := BuildProvider(ProviderConfig{Type: ""})
	if err != nil {
		t.Fatalf("BuildProvider empty type: %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
	a, ok := p.(*OpenAIAdapter)
	if !ok {
		t.Fatalf("expected *OpenAIAdapter, got %T", p)
	}
	if a.baseURL != "http://localhost:11434" {
		t.Errorf("expected Ollama default URL, got %q", a.baseURL)
	}
}

// TestBuildProvider_OllamaExplicit verifies that type "ollama" returns Ollama adapter.
func TestBuildProvider_OllamaExplicit(t *testing.T) {
	p, err := BuildProvider(ProviderConfig{Type: "ollama"})
	if err != nil {
		t.Fatalf("BuildProvider ollama: %v", err)
	}
	if p.Name() != "openai-compatible" {
		t.Errorf("unexpected name: %q", p.Name())
	}
}

// TestBuildProvider_OllamaCustomBaseURL verifies that a custom base URL is forwarded.
func TestBuildProvider_OllamaCustomBaseURL(t *testing.T) {
	p, err := BuildProvider(ProviderConfig{Type: "ollama", BaseURL: "http://remote:11434"})
	if err != nil {
		t.Fatalf("BuildProvider ollama custom URL: %v", err)
	}
	a := p.(*OpenAIAdapter)
	if a.baseURL != "http://remote:11434" {
		t.Errorf("expected custom base URL, got %q", a.baseURL)
	}
}

// TestBuildProvider_OpenAI_WithLiteralKey verifies the OpenAI adapter is created
// when a literal API key is provided.
func TestBuildProvider_OpenAI_WithLiteralKey(t *testing.T) {
	p, err := BuildProvider(ProviderConfig{Type: "openai", APIKey: "sk-live"})
	if err != nil {
		t.Fatalf("BuildProvider openai: %v", err)
	}
	a := p.(*OpenAIAdapter)
	if a.apiKey != "sk-live" {
		t.Errorf("expected apiKey='sk-live', got %q", a.apiKey)
	}
	if a.baseURL != "https://api.openai.com" {
		t.Errorf("expected api.openai.com, got %q", a.baseURL)
	}
}

// TestBuildProvider_OpenAI_EnvVarKey verifies that "$OPENAI_KEY" is resolved
// from the environment.
func TestBuildProvider_OpenAI_EnvVarKey(t *testing.T) {
	t.Setenv("OPENAI_KEY", "sk-from-env")
	p, err := BuildProvider(ProviderConfig{Type: "openai", APIKey: "$OPENAI_KEY"})
	if err != nil {
		t.Fatalf("BuildProvider openai env key: %v", err)
	}
	a := p.(*OpenAIAdapter)
	if a.apiKey != "sk-from-env" {
		t.Errorf("expected 'sk-from-env', got %q", a.apiKey)
	}
}

// TestBuildProvider_OpenAI_MissingKey verifies that an empty API key is rejected.
func TestBuildProvider_OpenAI_MissingKey(t *testing.T) {
	// Ensure the env var is unset so resolveEnv("$OPENAI_KEY") → ""
	os.Unsetenv("OPENAI_KEY")
	_, err := BuildProvider(ProviderConfig{Type: "openai", APIKey: ""})
	if err == nil {
		t.Fatal("expected error for missing OpenAI API key, got nil")
	}
	if !strings.Contains(err.Error(), "api_key") {
		t.Errorf("error should mention api_key, got: %v", err)
	}
}

// TestBuildProvider_UnknownType verifies that an unrecognised provider type
// returns a descriptive error.
func TestBuildProvider_UnknownType(t *testing.T) {
	_, err := BuildProvider(ProviderConfig{Type: "anthropic-not-implemented-yet"})
	if err == nil {
		t.Fatal("expected error for unknown provider type, got nil")
	}
	if !strings.Contains(err.Error(), "unknown provider type") {
		t.Errorf("error should mention 'unknown provider type', got: %v", err)
	}
}

// TestBuildProvider_CaseInsensitive verifies that "OLLAMA" and "OpenAI" are
// normalised to lowercase before matching.
func TestBuildProvider_CaseInsensitive(t *testing.T) {
	_, err := BuildProvider(ProviderConfig{Type: "OLLAMA"})
	if err != nil {
		t.Errorf("OLLAMA should be accepted, got: %v", err)
	}
}

// ---------------------------------------------------------------------------
// resolveEnv
// ---------------------------------------------------------------------------

// TestResolveEnv_LiteralString verifies that a plain string is returned as-is.
func TestResolveEnv_LiteralString(t *testing.T) {
	got := resolveEnv("plain-value")
	if got != "plain-value" {
		t.Errorf("expected 'plain-value', got %q", got)
	}
}

// TestResolveEnv_DollarPrefix_Set verifies that "$VAR" is substituted.
func TestResolveEnv_DollarPrefix_Set(t *testing.T) {
	t.Setenv("MY_SECRET", "abc123")
	got := resolveEnv("$MY_SECRET")
	if got != "abc123" {
		t.Errorf("expected 'abc123', got %q", got)
	}
}

// TestResolveEnv_DollarPrefix_Unset verifies that an unset "$VAR" becomes "".
func TestResolveEnv_DollarPrefix_Unset(t *testing.T) {
	os.Unsetenv("DOES_NOT_EXIST_XYZ")
	got := resolveEnv("$DOES_NOT_EXIST_XYZ")
	if got != "" {
		t.Errorf("expected empty string for unset var, got %q", got)
	}
}

// TestResolveEnv_EmptyString verifies an empty input returns empty output.
func TestResolveEnv_EmptyString(t *testing.T) {
	got := resolveEnv("")
	if got != "" {
		t.Errorf("expected empty string, got %q", got)
	}
}

// TestResolveEnv_DollarInMiddle verifies that "$" not at the start is kept as-is.
func TestResolveEnv_DollarInMiddle(t *testing.T) {
	got := resolveEnv("prefix$SUFFIX")
	if got != "prefix$SUFFIX" {
		t.Errorf("expected literal 'prefix$SUFFIX', got %q", got)
	}
}
