package config

import (
	"os"
	"testing"
)

var validYAML = []byte(`
version: "1"
name: test-pipeline
connectors:
  - name: my-source
    type: csv
    config:
      path: ./data.csv
  - name: my-sink
    type: postgres
    config:
      dsn: postgres://localhost/test
      table: public.output
tasks:
  - id: read
    connector: my-source
    depends_on: []
  - id: write
    connector: my-sink
    depends_on: [read]
`)

func TestLoadBytes_Valid(t *testing.T) {
	cfg, err := LoadBytes(validYAML)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Name != "test-pipeline" {
		t.Errorf("expected name test-pipeline, got %s", cfg.Name)
	}
	if len(cfg.Connectors) != 2 {
		t.Errorf("expected 2 connectors, got %d", len(cfg.Connectors))
	}
	if len(cfg.Tasks) != 2 {
		t.Errorf("expected 2 tasks, got %d", len(cfg.Tasks))
	}
}

func TestLoadBytes_MissingVersion(t *testing.T) {
	raw := []byte(`
name: test-pipeline
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for missing version, got nil")
	}
}

func TestLoadBytes_MissingName(t *testing.T) {
	raw := []byte(`
version: "1"
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for missing name, got nil")
	}
}

func TestLoadBytes_UnsupportedVersion(t *testing.T) {
	raw := []byte(`
version: "2"
name: test
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for unsupported version, got nil")
	}
}

func TestLoadBytes_DuplicateConnectorName(t *testing.T) {
	raw := []byte(`
version: "1"
name: test
connectors:
  - name: src
    type: csv
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for duplicate connector name, got nil")
	}
}

func TestLoadBytes_DuplicateTaskID(t *testing.T) {
	raw := []byte(`
version: "1"
name: test
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
  - id: read
    connector: src
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for duplicate task id, got nil")
	}
}

func TestLoadBytes_UnknownConnectorRef(t *testing.T) {
	raw := []byte(`
version: "1"
name: test
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: does-not-exist
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for unknown connector ref, got nil")
	}
}

func TestLoadBytes_UnknownTransformRef(t *testing.T) {
	raw := []byte(`
version: "1"
name: test
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
    transform: does-not-exist
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for unknown transform ref, got nil")
	}
}

func TestLoadBytes_UnknownDependsOn(t *testing.T) {
	raw := []byte(`
version: "1"
name: test
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
    depends_on: [ghost]
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for unknown depends_on ref, got nil")
	}
}

func TestLoadBytes_TaskWithNoConnectorOrTransform(t *testing.T) {
	raw := []byte(`
version: "1"
name: test
connectors:
  - name: src
    type: csv
tasks:
  - id: read
    connector: src
  - id: empty
    depends_on: [read]
`)
	_, err := LoadBytes(raw)
	if err == nil {
		t.Fatal("expected error for task with no connector or transform, got nil")
	}
}

func TestLoadBytes_EnvSubstitution(t *testing.T) {
	os.Setenv("TEST_DSN", "postgres://user:pass@localhost/db")
	defer os.Unsetenv("TEST_DSN")

	raw := []byte(`
version: "1"
name: test
connectors:
  - name: sink
    type: postgres
    config:
      dsn: $TEST_DSN
tasks:
  - id: write
    connector: sink
`)
	cfg, err := LoadBytes(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	dsn, _ := cfg.Connectors[0].Config["dsn"].(string)
	if dsn != "postgres://user:pass@localhost/db" {
		t.Errorf("expected DSN to be substituted, got %q", dsn)
	}
}
