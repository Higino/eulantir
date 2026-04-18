package agent

import (
	"strings"
	"testing"

	"github.com/higino/eulantir/internal/connector"
)

// TestSystemPrompt_ContainsRequiredSections checks that the prompt includes
// the fundamental structural keywords a well-formed system prompt needs.
func TestSystemPrompt_ContainsRequiredSections(t *testing.T) {
	prompt := SystemPrompt(nil)

	must := []string{
		"Pipeline YAML schema",
		"Transform Go interface",
		"package main",
		"connector.Record",
		"version:",
		"connectors:",
		"tasks:",
	}
	for _, want := range must {
		if !strings.Contains(prompt, want) {
			t.Errorf("SystemPrompt missing expected substring %q", want)
		}
	}
}

// TestSystemPrompt_EmptyCatalog verifies that an empty catalog produces a
// graceful placeholder rather than panicking or emitting blank output.
func TestSystemPrompt_EmptyCatalog(t *testing.T) {
	prompt := SystemPrompt(nil)
	if !strings.Contains(prompt, "no connectors registered") {
		t.Errorf("expected empty-catalog placeholder, got: %q", prompt[len(prompt)-200:])
	}
}

// TestSystemPrompt_CatalogIncluded verifies that registered connector types,
// descriptions, and config keys all appear in the generated prompt.
func TestSystemPrompt_CatalogIncluded(t *testing.T) {
	catalog := []connector.ConnectorInfo{
		{Type: "csv", Description: "Read records from a CSV file", ConfigKeys: []string{"path", "delimiter"}},
		{Type: "postgres", Description: "Write records to PostgreSQL", ConfigKeys: []string{"dsn", "table"}},
	}
	prompt := SystemPrompt(catalog)

	for _, want := range []string{"csv", "postgres", "Read records from a CSV file", "path", "delimiter", "dsn", "table"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt missing %q", want)
		}
	}
}

// TestSystemPrompt_MultipleCatalogs verifies all connector entries are present
// when more than two connectors are registered.
func TestSystemPrompt_MultipleCatalogs(t *testing.T) {
	catalog := []connector.ConnectorInfo{
		{Type: "alpha", Description: "Alpha connector", ConfigKeys: []string{"a_key"}},
		{Type: "beta", Description: "Beta connector", ConfigKeys: []string{"b_key"}},
		{Type: "gamma", Description: "Gamma connector", ConfigKeys: []string{"g_key"}},
	}
	prompt := SystemPrompt(catalog)
	for _, c := range catalog {
		if !strings.Contains(prompt, c.Type) {
			t.Errorf("prompt missing connector type %q", c.Type)
		}
		if !strings.Contains(prompt, c.Description) {
			t.Errorf("prompt missing connector description %q", c.Description)
		}
		for _, k := range c.ConfigKeys {
			if !strings.Contains(prompt, k) {
				t.Errorf("prompt missing config key %q for connector %q", k, c.Type)
			}
		}
	}
}

// TestSystemPrompt_PackageMainRule verifies that the strict "package main"
// rule appears in the prompt so the LLM knows the plugin constraint.
func TestSystemPrompt_PackageMainRule(t *testing.T) {
	prompt := SystemPrompt(nil)
	if !strings.Contains(prompt, `MUST be "package main"`) {
		t.Error("prompt is missing the 'MUST be package main' rule")
	}
}

// TestBuildCatalogBlock_Empty verifies the fallback string for empty catalog.
func TestBuildCatalogBlock_Empty(t *testing.T) {
	got := buildCatalogBlock(nil)
	if got != "(no connectors registered)" {
		t.Errorf("unexpected empty catalog block: %q", got)
	}
}

// TestBuildCatalogBlock_SingleConnector verifies format for one entry.
func TestBuildCatalogBlock_SingleConnector(t *testing.T) {
	catalog := []connector.ConnectorInfo{
		{Type: "csv-sink", Description: "Write CSV", ConfigKeys: []string{"path"}},
	}
	got := buildCatalogBlock(catalog)
	if !strings.Contains(got, "csv-sink") {
		t.Error("missing type in catalog block")
	}
	if !strings.Contains(got, "Write CSV") {
		t.Error("missing description in catalog block")
	}
	if !strings.Contains(got, "path") {
		t.Error("missing config key in catalog block")
	}
}

// TestBuildCatalogBlock_MultipleConfigKeys verifies comma-separation of keys.
func TestBuildCatalogBlock_MultipleConfigKeys(t *testing.T) {
	catalog := []connector.ConnectorInfo{
		{Type: "postgres", Description: "Postgres sink", ConfigKeys: []string{"dsn", "table", "upsert_key"}},
	}
	got := buildCatalogBlock(catalog)
	// All three keys must appear in the block, separated by ", "
	if !strings.Contains(got, "dsn, table, upsert_key") {
		t.Errorf("expected comma-separated keys, got: %q", got)
	}
}
