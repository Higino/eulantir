package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-playground/validator/v10"
	"gopkg.in/yaml.v3"
)

var validate = validator.New()

// Load reads a pipeline YAML file, substitutes $ENV_VAR references,
// unmarshals it into a PipelineConfig, and runs structural validation.
func Load(path string) (*PipelineConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}

	expanded := expandEnv(string(raw))

	var cfg PipelineConfig
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	// Resolve transform source paths relative to the YAML file's directory.
	// This lets pipeline.yaml use relative paths like "generated/filter.go"
	// regardless of the working directory when eulantir is invoked.
	yamlDir := filepath.Dir(path)
	for i := range cfg.Transforms {
		if !filepath.IsAbs(cfg.Transforms[i].Source) {
			cfg.Transforms[i].Source = filepath.Join(yamlDir, cfg.Transforms[i].Source)
		}
	}

	return &cfg, nil
}

// LoadBytes parses a pipeline config from a raw YAML byte slice.
// Useful for validating LLM-generated YAML before writing to disk.
func LoadBytes(raw []byte) (*PipelineConfig, error) {
	expanded := expandEnv(string(raw))

	var cfg PipelineConfig
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse YAML: %w", err)
	}

	if err := validateConfig(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// expandEnv replaces $VAR and ${VAR} patterns with their environment values.
// Unknown variables are left as empty strings (os.ExpandEnv behaviour).
func expandEnv(s string) string {
	return os.ExpandEnv(s)
}

// validateConfig runs all validation rules on a loaded PipelineConfig.
func validateConfig(cfg *PipelineConfig) error {
	// 1. struct-tag validation (required, min, etc.)
	if err := validate.Struct(cfg); err != nil {
		return formatValidationError(err)
	}

	// 2. version check
	if cfg.Version != "1" {
		return fmt.Errorf("unsupported version %q — only \"1\" is supported", cfg.Version)
	}

	// 3. unique connector names
	connectorNames := make(map[string]struct{}, len(cfg.Connectors))
	for _, c := range cfg.Connectors {
		if _, dup := connectorNames[c.Name]; dup {
			return fmt.Errorf("duplicate connector name %q", c.Name)
		}
		connectorNames[c.Name] = struct{}{}
	}

	// 4. unique transform names
	transformNames := make(map[string]struct{}, len(cfg.Transforms))
	for _, t := range cfg.Transforms {
		if _, dup := transformNames[t.Name]; dup {
			return fmt.Errorf("duplicate transform name %q", t.Name)
		}
		transformNames[t.Name] = struct{}{}
	}

	// 5. unique task IDs + cross-reference checks
	taskIDs := make(map[string]struct{}, len(cfg.Tasks))
	for _, task := range cfg.Tasks {
		if task.ID == "" {
			return fmt.Errorf("task is missing an id field")
		}
		if _, dup := taskIDs[task.ID]; dup {
			return fmt.Errorf("duplicate task id %q", task.ID)
		}
		taskIDs[task.ID] = struct{}{}

		// connector ref must exist
		if task.Connector != "" {
			if _, ok := connectorNames[task.Connector]; !ok {
				return fmt.Errorf("task %q references unknown connector %q", task.ID, task.Connector)
			}
		}

		// transform ref must exist
		if task.Transform != "" {
			if _, ok := transformNames[task.Transform]; !ok {
				return fmt.Errorf("task %q references unknown transform %q", task.ID, task.Transform)
			}
		}

		// a task must do something
		if task.Connector == "" && task.Transform == "" {
			return fmt.Errorf("task %q has neither a connector nor a transform", task.ID)
		}
	}

	// 6. depends_on references must point to known task IDs
	for _, task := range cfg.Tasks {
		for _, dep := range task.DependsOn {
			if _, ok := taskIDs[dep]; !ok {
				return fmt.Errorf("task %q depends_on unknown task %q", task.ID, dep)
			}
		}
	}

	return nil
}

// formatValidationError converts go-playground validator errors into
// readable messages without the verbose struct path prefix.
func formatValidationError(err error) error {
	var msgs []string
	for _, e := range err.(validator.ValidationErrors) {
		field := e.Field()
		switch e.Tag() {
		case "required":
			msgs = append(msgs, fmt.Sprintf("field %q is required", field))
		case "min":
			msgs = append(msgs, fmt.Sprintf("field %q must have at least %s item(s)", field, e.Param()))
		default:
			msgs = append(msgs, fmt.Sprintf("field %q failed %q validation", field, e.Tag()))
		}
	}
	return fmt.Errorf("validation errors:\n  - %s", strings.Join(msgs, "\n  - "))
}
