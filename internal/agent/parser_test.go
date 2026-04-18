package agent

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Happy-path tests
// ---------------------------------------------------------------------------

// TestParse_YAMLOnly verifies a response with only a YAML block (no transforms).
func TestParse_YAMLOnly(t *testing.T) {
	raw := "```yaml\nversion: \"1\"\nname: test\n```"
	out, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if out.PipelineYAML != "version: \"1\"\nname: test" {
		t.Errorf("unexpected PipelineYAML: %q", out.PipelineYAML)
	}
	if len(out.GoFiles) != 0 {
		t.Errorf("expected no Go files, got %d", len(out.GoFiles))
	}
}

// TestParse_YAMLAndOneGoBlock verifies that a valid transform header is
// extracted and the filename is derived from the transform name.
func TestParse_YAMLAndOneGoBlock(t *testing.T) {
	raw := "```yaml\nversion: \"1\"\n```\n\n```go\n// transform: filter_active\npackage main\n\nfunc FilterActive() {}\n```"
	out, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, ok := out.GoFiles["filter_active.go"]; !ok {
		t.Errorf("expected key 'filter_active.go' in GoFiles, got keys: %v", keys(out.GoFiles))
	}
	if !strings.Contains(out.GoFiles["filter_active.go"], "FilterActive") {
		t.Errorf("Go file content missing FilterActive function")
	}
}

// TestParse_MultipleGoBlocks verifies that multiple Go blocks are each
// stored under their own filename.
func TestParse_MultipleGoBlocks(t *testing.T) {
	raw := "```yaml\nversion: \"1\"\n```\n\n" +
		"```go\n// transform: clean_nulls\npackage main\nfunc A() {}\n```\n\n" +
		"```go\n// transform: uppercase_name\npackage main\nfunc B() {}\n```"
	out, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(out.GoFiles) != 2 {
		t.Fatalf("expected 2 Go files, got %d: %v", len(out.GoFiles), keys(out.GoFiles))
	}
	for _, name := range []string{"clean_nulls.go", "uppercase_name.go"} {
		if _, ok := out.GoFiles[name]; !ok {
			t.Errorf("expected key %q in GoFiles", name)
		}
	}
}

// TestParse_TransformHeaderSlashFormat verifies the alternate
// "// transform/name.go" header format is also accepted.
func TestParse_TransformHeaderSlashFormat(t *testing.T) {
	raw := "```yaml\nversion: \"1\"\n```\n\n```go\n// transform/my_filter.go\npackage main\n```"
	out, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if _, ok := out.GoFiles["my_filter.go"]; !ok {
		t.Errorf("expected 'my_filter.go', got keys: %v", keys(out.GoFiles))
	}
}

// TestParse_LeadingAndTrailingWhitespace verifies that extra whitespace
// around the YAML content is trimmed.
func TestParse_LeadingAndTrailingWhitespace(t *testing.T) {
	raw := "```yaml\n\n  version: \"1\"\n\n```"
	out, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if strings.HasPrefix(out.PipelineYAML, "\n") || strings.HasSuffix(out.PipelineYAML, "\n") {
		t.Errorf("PipelineYAML should be trimmed, got: %q", out.PipelineYAML)
	}
}

// ---------------------------------------------------------------------------
// Error-path tests
// ---------------------------------------------------------------------------

// TestParse_NoYAMLBlock verifies an error is returned when no YAML block exists.
func TestParse_NoYAMLBlock(t *testing.T) {
	_, err := Parse("no code blocks here")
	if err == nil {
		t.Fatal("expected error for missing YAML block, got nil")
	}
	if !strings.Contains(err.Error(), "no ```yaml block") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestParse_MultipleYAMLBlocks verifies an error when more than one YAML block
// is present — the agent must produce exactly one.
func TestParse_MultipleYAMLBlocks(t *testing.T) {
	raw := "```yaml\nversion: \"1\"\n```\n```yaml\nname: dupe\n```"
	_, err := Parse(raw)
	if err == nil {
		t.Fatal("expected error for multiple YAML blocks, got nil")
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("error should mention count, got: %v", err)
	}
}

// TestParse_GoBlockMissingHeader verifies an error when a Go block lacks the
// required "// transform: <name>" first-line comment.
func TestParse_GoBlockMissingHeader(t *testing.T) {
	raw := "```yaml\nversion: \"1\"\n```\n\n```go\npackage main\n// no header here\n```"
	_, err := Parse(raw)
	if err == nil {
		t.Fatal("expected error for Go block without transform header, got nil")
	}
	if !strings.Contains(err.Error(), "missing required header") {
		t.Errorf("unexpected error message: %v", err)
	}
}

// TestParse_EmptyGoBlock verifies that a Go block whose first line is empty
// also fails with a clear error.
func TestParse_EmptyGoBlock(t *testing.T) {
	raw := "```yaml\nversion: \"1\"\n```\n\n```go\n\npackage main\n```"
	_, err := Parse(raw)
	if err == nil {
		t.Fatal("expected error for empty-first-line Go block, got nil")
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
