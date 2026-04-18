package llm

import (
	"fmt"
	"os"
	"strings"
)

// ProviderConfig is the llm: section of pipeline.yaml.
type ProviderConfig struct {
	Type    string `yaml:"provider"`  // ollama | openai | anthropic
	BaseURL string `yaml:"base_url"`
	APIKey  string `yaml:"api_key"`   // supports $ENV_VAR substitution
	Model   string `yaml:"model"`
}

// BuildProvider constructs the right Provider from config.
// API keys prefixed with "$" are resolved from environment variables.
func BuildProvider(cfg ProviderConfig) (Provider, error) {
	apiKey := resolveEnv(cfg.APIKey)

	switch strings.ToLower(cfg.Type) {
	case "ollama", "":
		return NewOllamaAdapter(cfg.BaseURL), nil
	case "openai":
		if apiKey == "" {
			return nil, fmt.Errorf("openai provider requires api_key (or $OPENAI_API_KEY)")
		}
		return NewOpenAIAdapter(apiKey), nil
	case "anthropic":
		if apiKey == "" {
			return nil, fmt.Errorf("anthropic provider requires api_key (or $ANTHROPIC_API_KEY)")
		}
		return NewAnthropicAdapter(apiKey), nil
	default:
		return nil, fmt.Errorf("unknown provider type %q — valid options: ollama, openai, anthropic", cfg.Type)
	}
}

// resolveEnv substitutes "$ENV_VAR" values with their environment variable value.
func resolveEnv(s string) string {
	if strings.HasPrefix(s, "$") {
		return os.Getenv(strings.TrimPrefix(s, "$"))
	}
	return s
}
