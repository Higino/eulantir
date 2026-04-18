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

// openAIRequest is the JSON body sent to any OpenAI-compatible endpoint.
type openAIRequest struct {
	Model       string          `json:"model"`
	Messages    []openAIMessage `json:"messages"`
	MaxTokens   int             `json:"max_tokens,omitempty"`
	Temperature float64         `json:"temperature,omitempty"`
}

type openAIMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIResponse struct {
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int `json:"prompt_tokens"`
		CompletionTokens int `json:"completion_tokens"`
	} `json:"usage"`
}

// OpenAIAdapter implements Provider for any OpenAI-compatible endpoint.
// Use NewOllamaAdapter for Ollama and NewOpenAIAdapter for OpenAI proper.
type OpenAIAdapter struct {
	baseURL    string
	apiKey     string
	httpClient *http.Client
}

// NewOllamaAdapter returns an adapter pointed at a local Ollama instance.
// baseURL defaults to http://localhost:11434 if empty.
func NewOllamaAdapter(baseURL string) *OpenAIAdapter {
	if baseURL == "" {
		baseURL = "http://localhost:11434"
	}
	return &OpenAIAdapter{
		baseURL: baseURL,
		apiKey:  "ollama", // Ollama ignores the key but the header must be present
		httpClient: &http.Client{
			Timeout: 300 * time.Second, // local models can be slow
		},
	}
}

// NewOpenAIAdapter returns an adapter pointed at api.openai.com.
func NewOpenAIAdapter(apiKey string) *OpenAIAdapter {
	return &OpenAIAdapter{
		baseURL: "https://api.openai.com",
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
	}
}

func (a *OpenAIAdapter) Name() string { return "openai-compatible" }

func (a *OpenAIAdapter) Complete(ctx context.Context, req CompletionRequest) (CompletionResponse, error) {
	// Convert to OpenAI message format
	msgs := make([]openAIMessage, len(req.Messages))
	for i, m := range req.Messages {
		msgs[i] = openAIMessage{Role: string(m.Role), Content: m.Content}
	}

	body := openAIRequest{
		Model:       req.Model,
		Messages:    msgs,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
	}

	b, err := json.Marshal(body)
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.baseURL+"/v1/chat/completions", bytes.NewReader(b))
	if err != nil {
		return CompletionResponse{}, fmt.Errorf("create request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+a.apiKey)

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
		return CompletionResponse{}, fmt.Errorf("provider returned %d: %s", resp.StatusCode, string(raw))
	}

	var oaiResp openAIResponse
	if err := json.Unmarshal(raw, &oaiResp); err != nil {
		return CompletionResponse{}, fmt.Errorf("unmarshal response: %w", err)
	}

	if len(oaiResp.Choices) == 0 {
		return CompletionResponse{}, fmt.Errorf("provider returned no choices")
	}

	return CompletionResponse{
		Content:      oaiResp.Choices[0].Message.Content,
		Model:        oaiResp.Model,
		InputTokens:  oaiResp.Usage.PromptTokens,
		OutputTokens: oaiResp.Usage.CompletionTokens,
	}, nil
}
