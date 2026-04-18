package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// successAnthropicBody builds a valid Anthropic Messages API response body.
func successAnthropicBody(text string) string {
	resp := anthropicResponse{
		ID:    "msg_test",
		Model: "claude-opus-4-5",
		Content: []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{{Type: "text", Text: text}},
	}
	resp.Usage.InputTokens = 15
	resp.Usage.OutputTokens = 25
	b, _ := json.Marshal(resp)
	return string(b)
}

// newAnthropicTestAdapter creates an AnthropicAdapter pointed at srv.
func newAnthropicTestAdapter(srv *httptest.Server) *AnthropicAdapter {
	return &AnthropicAdapter{
		baseURL:    srv.URL,
		apiKey:     "test-key",
		httpClient: srv.Client(),
	}
}

// ---------------------------------------------------------------------------
// Constructor / Name
// ---------------------------------------------------------------------------

func TestNewAnthropicAdapter_Name(t *testing.T) {
	if NewAnthropicAdapter("key").Name() != "anthropic" {
		t.Error("expected Name()='anthropic'")
	}
}

func TestNewAnthropicAdapter_BaseURL(t *testing.T) {
	a := NewAnthropicAdapter("key")
	if a.baseURL != anthropicBaseURL {
		t.Errorf("expected baseURL=%q, got %q", anthropicBaseURL, a.baseURL)
	}
}

func TestNewAnthropicAdapter_StoresKey(t *testing.T) {
	a := NewAnthropicAdapter("sk-ant-test")
	if a.apiKey != "sk-ant-test" {
		t.Errorf("expected apiKey='sk-ant-test', got %q", a.apiKey)
	}
}

// ---------------------------------------------------------------------------
// BuildProvider — anthropic branch
// ---------------------------------------------------------------------------

func TestBuildProvider_Anthropic_WithKey(t *testing.T) {
	p, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: "sk-ant-live"})
	if err != nil {
		t.Fatalf("BuildProvider anthropic: %v", err)
	}
	if _, ok := p.(*AnthropicAdapter); !ok {
		t.Errorf("expected *AnthropicAdapter, got %T", p)
	}
}

func TestBuildProvider_Anthropic_MissingKey(t *testing.T) {
	_, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: ""})
	if err == nil {
		t.Fatal("expected error for missing Anthropic API key, got nil")
	}
	if !strings.Contains(err.Error(), "api_key") {
		t.Errorf("error should mention api_key, got: %v", err)
	}
}

func TestBuildProvider_Anthropic_EnvVarKey(t *testing.T) {
	t.Setenv("ANTHROPIC_API_KEY", "sk-ant-from-env")
	p, err := BuildProvider(ProviderConfig{Type: "anthropic", APIKey: "$ANTHROPIC_API_KEY"})
	if err != nil {
		t.Fatalf("BuildProvider anthropic env key: %v", err)
	}
	if p.(*AnthropicAdapter).apiKey != "sk-ant-from-env" {
		t.Error("api key not resolved from env")
	}
}

// ---------------------------------------------------------------------------
// Complete — happy paths
// ---------------------------------------------------------------------------

func TestAnthropicComplete_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(successAnthropicBody("Hello from Claude")))
	}))
	defer srv.Close()

	resp, err := newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{
		Model:    "claude-opus-4-5",
		Messages: []Message{{Role: RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if resp.Content != "Hello from Claude" {
		t.Errorf("expected 'Hello from Claude', got %q", resp.Content)
	}
	if resp.InputTokens != 15 || resp.OutputTokens != 25 {
		t.Errorf("unexpected token counts: in=%d out=%d", resp.InputTokens, resp.OutputTokens)
	}
}

func TestAnthropicComplete_SystemMessageSeparated(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(successAnthropicBody("ok")))
	}))
	defer srv.Close()

	newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{
		Model: "m",
		Messages: []Message{
			{Role: RoleSystem, Content: "you are helpful"},
			{Role: RoleUser, Content: "hello"},
		},
	})

	if gotBody["system"] != "you are helpful" {
		t.Errorf("expected system='you are helpful', got %v", gotBody["system"])
	}
	msgs, _ := gotBody["messages"].([]any)
	if len(msgs) != 1 {
		t.Errorf("expected 1 message in messages array (system excluded), got %d", len(msgs))
	}
}

func TestAnthropicComplete_SetsRequiredHeaders(t *testing.T) {
	var gotAPIKey, gotVersion, gotCT string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("x-api-key")
		gotVersion = r.Header.Get("anthropic-version")
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(successAnthropicBody("ok")))
	}))
	defer srv.Close()

	newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{Model: "m"})

	if gotAPIKey != "test-key" {
		t.Errorf("expected x-api-key='test-key', got %q", gotAPIKey)
	}
	if gotVersion != anthropicVersion {
		t.Errorf("expected anthropic-version=%q, got %q", anthropicVersion, gotVersion)
	}
	if gotCT != "application/json" {
		t.Errorf("expected Content-Type='application/json', got %q", gotCT)
	}
}

func TestAnthropicComplete_DefaultMaxTokens(t *testing.T) {
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(successAnthropicBody("ok")))
	}))
	defer srv.Close()

	newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{
		Model: "m", MaxTokens: 0,
	})

	if gotBody["max_tokens"] != float64(defaultMaxTokens) {
		t.Errorf("expected max_tokens=%d, got %v", defaultMaxTokens, gotBody["max_tokens"])
	}
}

func TestAnthropicComplete_HitsMessagesPath(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(successAnthropicBody("ok")))
	}))
	defer srv.Close()

	newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{Model: "m"})

	if gotPath != "/v1/messages" {
		t.Errorf("expected path /v1/messages, got %q", gotPath)
	}
}

// ---------------------------------------------------------------------------
// Complete — error paths
// ---------------------------------------------------------------------------

func TestAnthropicComplete_Non200SurfacesMessage(t *testing.T) {
	body := `{"type":"error","error":{"type":"authentication_error","message":"invalid api key"}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(body))
	}))
	defer srv.Close()

	_, err := newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{Model: "m"})
	if err == nil {
		t.Fatal("expected error for 401, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error should mention 401, got: %v", err)
	}
}

func TestAnthropicComplete_500Error(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"internal"}`))
	}))
	defer srv.Close()

	_, err := newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{Model: "m"})
	if err == nil {
		t.Fatal("expected error for 500, got nil")
	}
}

func TestAnthropicComplete_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("not-json"))
	}))
	defer srv.Close()

	_, err := newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{Model: "m"})
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Errorf("expected unmarshal error, got: %v", err)
	}
}

func TestAnthropicComplete_EmptyContent(t *testing.T) {
	body := `{"id":"msg_1","model":"m","content":[],"usage":{"input_tokens":0,"output_tokens":0}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(body))
	}))
	defer srv.Close()

	_, err := newAnthropicTestAdapter(srv).Complete(context.Background(), CompletionRequest{Model: "m"})
	if err == nil {
		t.Fatal("expected error for empty content, got nil")
	}
	if !strings.Contains(err.Error(), "no text content") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestAnthropicComplete_NetworkError(t *testing.T) {
	a := &AnthropicAdapter{
		baseURL:    "http://127.0.0.1:1",
		apiKey:     "key",
		httpClient: &http.Client{},
	}
	_, err := a.Complete(context.Background(), CompletionRequest{Model: "m"})
	if err == nil {
		t.Fatal("expected error for refused connection, got nil")
	}
	if !strings.Contains(err.Error(), "http request") {
		t.Errorf("expected 'http request' in error, got: %v", err)
	}
}

func TestAnthropicComplete_ContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := newAnthropicTestAdapter(srv).Complete(ctx, CompletionRequest{Model: "m"})
	if err == nil {
		t.Fatal("expected error for cancelled context, got nil")
	}
}
