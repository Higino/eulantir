package config

// PipelineConfig is the top-level structure of a pipeline.yaml file.
type PipelineConfig struct {
	Version   string          `yaml:"version"   validate:"required"`
	Name      string          `yaml:"name"       validate:"required"`
	Meta      Meta            `yaml:"meta"`
	LLM       LLMConfig       `yaml:"llm"`
	Execution ExecutionConfig `yaml:"execution"`
	Lineage   LineageConfig   `yaml:"lineage"`
	Connectors []ConnectorConfig `yaml:"connectors" validate:"required,min=1"`
	Transforms []TransformConfig `yaml:"transforms"`
	Tasks      []TaskConfig    `yaml:"tasks"      validate:"required,min=1"`
	Retry      RetryConfig     `yaml:"retry"`
	DLQ        DLQConfig       `yaml:"dlq"`
}

// Meta holds optional descriptive metadata about the pipeline.
type Meta struct {
	Description string   `yaml:"description"`
	Owner       string   `yaml:"owner"`
	Tags        []string `yaml:"tags"`
}

// LLMConfig configures which LLM provider and model to use for generation.
type LLMConfig struct {
	Provider string `yaml:"provider"` // ollama | openai | anthropic
	Model    string `yaml:"model"`
	BaseURL  string `yaml:"base_url"` // optional override (useful for Ollama)
	APIKey   string `yaml:"api_key"`  // supports $ENV_VAR substitution
}

// ExecutionConfig controls runtime behaviour of the pipeline engine.
type ExecutionConfig struct {
	MaxConcurrency     int    `yaml:"max_concurrency"`      // max parallel task nodes (default: 4)
	BatchSize          int    `yaml:"batch_size"`           // records per ReadBatch call (default: 1000)
	MicrobatchInterval string `yaml:"microbatch_interval"`  // flush interval e.g. "5s"
}

// LineageConfig controls OpenLineage event emission.
type LineageConfig struct {
	Enabled   bool   `yaml:"enabled"`
	Endpoint  string `yaml:"endpoint"`  // e.g. http://localhost:5000 (Marquez)
	Namespace string `yaml:"namespace"`
}

// ConnectorConfig declares a named connector instance and its type-specific config.
type ConnectorConfig struct {
	Name   string         `yaml:"name"   validate:"required"`
	Type   string         `yaml:"type"   validate:"required"`
	Config map[string]any `yaml:"config"`
}

// TransformConfig points to a generated (or hand-written) Go transform file.
type TransformConfig struct {
	Name       string `yaml:"name"       validate:"required"`
	Source     string `yaml:"source"     validate:"required"` // path to .go file
	Entrypoint string `yaml:"entrypoint" validate:"required"` // exported function name
}

// TaskConfig is a single node in the pipeline DAG.
type TaskConfig struct {
	ID           string   `yaml:"id"         validate:"required"`
	Connector    string   `yaml:"connector"`  // name ref to ConnectorConfig
	Transform    string   `yaml:"transform"`  // name ref to TransformConfig (optional)
	DependsOn    []string `yaml:"depends_on"`
}

// RetryConfig controls retry behaviour for failed task executions.
type RetryConfig struct {
	MaxAttempts     int     `yaml:"max_attempts"`     // default: 3
	InitialInterval string  `yaml:"initial_interval"` // default: "1s"
	Multiplier      float64 `yaml:"multiplier"`       // default: 2.0
	MaxInterval     string  `yaml:"max_interval"`     // default: "30s"
	Jitter          bool    `yaml:"jitter"`           // default: true
}

// DLQConfig controls dead-letter queue behaviour.
type DLQConfig struct {
	Enabled bool           `yaml:"enabled"`
	Type    string         `yaml:"type"`   // file | (future: kafka, sqs)
	Config  map[string]any `yaml:"config"`
}
