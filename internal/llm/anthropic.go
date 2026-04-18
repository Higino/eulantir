package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	anthropicBaseURL = "https://api.anthropic.com"
	anthropicVersion = "2023-06-01"
	// defaultMaxTokens is used when the caller does not specify MaxTokens.
	// Anthropic requires max_tokens; there is no server-side default.
	defaultMaxTokens = 4096
)

// anthropicRequest is the JSON body sent to Anthropic's Messages API.
type anthropicRequest struct {
	Model     string           `json:"model"`
	MaxTokens int              `json:"max_tokens"`
	System    string           `json:"system,omitempty"`
	Messages  []anthropicMsg   `json:"messages"`
}

type anthropicMsg struct {
	Role    string `json:"role"`    // "user" | "assistant"
	Content string `json:"content"`
}

// anthropicResponse is the JSON body returned by Anthropic's Messages API.
type anthropicResponse struct {
	ID    string `json:"id"`
	Model string `json:"model"`
	// Content is an array of content blocks; we only use the first text block.
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Usage struct {
		InputTokens  int `json:"input_tokens"`
		OutputTokens int `json:"output_tokens"`
	} `json:"usage"`
	// Error is populated when the API returns a non-200 status.
	Error *struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// AnthropicAdapter implements Provider for Anthropic's Claude models.
// It calls the Messages API at https://api.anthropic.com/v1/messages.
//
// Key differences from the OpenAI-compatible endpoint:
//   - System messages are extracted and sent in a separate top-level "system" field.
//   - Auth uses the "x-api-key" header, not "Authorization: Bearer".
//   - The "anthropic-version" header is required.
//   - "max_tokens" is mandatory; defaults to 4096 if not specified.
type AnthropicAdapter struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewAnthropicAdapter returns an adapter for Anthropic's Claude API.
func NewAnthropicAdapter(apiKey string) *AnthropicAdapter {
	return &AnthropicAdapter{
		baseURL: anthropicBaseURL,
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

func (a *AnthropicAdapter) Name() string { return "anthropic" }

func (a *AnthropicAdapter) Complete(ctx context.Context, req CompletionRequest) (CompletionResponse, error) {
	// Anthropic separates the system prompt from the conversation turns.
	var system string
	var msgs []anthropicMsg
	for _, m := range req.Messages {
		switch m.Role {
		case RoleSystem:
			system = m.Content // last system message wins if there are multiple
		default:
			msgs = append(msgs, anthropicMsg{Role: string(m.Role), Content: m.Content})
		}
	}

	maxTokens := req.MaxTokens
	if maxTokens <= 0 {
		maxTokens = defaultMaxTokens
	}

	body := anthropicRequest{
		Model:     req.Model,
		MaxTokens: maxTokens,
		System:    system,
		Messages:  msgs,
	}

	b, err := json.Marshal(body)
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.baseURL+"/v1/messages", bytes.NewReader(b))
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", a.apiKey)
	httpReq.Header.Set("anthropic-version", anthropicVersion)

	resp, err := a.httpClient.Do(httpReq)
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// Try to surface the Anthropic error message if present.
		var errBody anthropicResponse
		if jerr := json.Unmarshal(raw, &errBody); jerr == nil && errBody.Error != nil {
			return CompletionResponse{}, fmt.Errorf("provider returned %d: %s",
				resp.StatusCode, errBody.Error.Message)
		}
		return CompletionResponse{}, fmt.Errorf("provider returned %d: %s",
			resp.StatusCode, string(raw))
	}

	var anthResp anthropicResponse
	if err := json.Unmarshal(raw, &anthResp); err != nil {
		return CompletionResponse{}, fmt.Errorf("unmarshal response: %w", err)
	}

	// Find the first text content block.
	var content string
	for _, block := range anthResp.Content {
		if block.Type == "text" {
			content = block.Text
			break
		}
	}
	if content == "" {
		return CompletionResponse{}, fmt.Errorf("provider returned no text content")
	}

	return CompletionResponse{
		Content:      content,
		Model:        anthResp.Model,
		InputTokens:  anthResp.Usage.InputTokens,
		OutputTokens: anthResp.Usage.OutputTokens,
	}, nil
}
