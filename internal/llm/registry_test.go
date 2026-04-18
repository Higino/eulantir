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
// API key env-var expansion (via BuildProvider)
// ---------------------------------------------------------------------------

// TestBuildProvider_APIKey_DollarVar verifies that "$VAR" is expanded.
func TestBuildProvider_APIKey_DollarVar(t *testing.T) {
	t.Setenv("MY_SECRET", "abc123")
	p, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: "$MY_SECRET"})
	if err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}

// TestBuildProvider_APIKey_BraceVar verifies that "${VAR}" is expanded.
func TestBuildProvider_APIKey_BraceVar(t *testing.T) {
	t.Setenv("MY_SECRET", "abc123")
	p, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: "${MY_SECRET}"})
	if err != nil {
		t.Fatalf("expected success with ${VAR} expansion, got %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}

// TestBuildProvider_APIKey_UnsetVar verifies that an unset "$VAR" makes the key empty.
func TestBuildProvider_APIKey_UnsetVar(t *testing.T) {
	os.Unsetenv("DOES_NOT_EXIST_XYZ")
	_, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: "$DOES_NOT_EXIST_XYZ"})
	if err == nil {
		t.Fatal("expected error when env var is unset and key is required")
	}
}

// TestBuildProvider_APIKey_LiteralValue verifies that a plain (non-$) key is used as-is.
func TestBuildProvider_APIKey_LiteralValue(t *testing.T) {
	p, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: "plain-key"})
	if err != nil {
		t.Fatalf("expected success with literal key, got %v", err)
	}
	if p == nil {
		t.Fatal("expected non-nil provider")
	}
}

// TestBuildProvider_APIKey_EmptyRejectsRequired verifies that an empty key is rejected for providers that require one.
func TestBuildProvider_APIKey_EmptyRejectsRequired(t *testing.T) {
	_, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: ""})
	if err == nil {
		t.Fatal("expected error for empty required API key")
	}
}
