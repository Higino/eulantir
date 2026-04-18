package agent

import (
	"fmt"
	"regexp"
	"strings"
)

var (
	// matches ```yaml ... ``` blocks
	yamlBlockRe = regexp.MustCompile("(?s)```yaml\\s*\\n(.*?)\\n?```")
	// matches ```go ... ``` blocks
	goBlockRe = regexp.MustCompile("(?s)```go\\s*\\n(.*?)\\n?```")
	// matches first-line transform hints in several formats the LLM may produce:
	//   // transform: clean_nulls
	//   // transform/clean_nulls.go
	//   // transform: clean_nulls.go
	transformNameRe = regexp.MustCompile(`^//\s*transform[:/]\s*([^\s.]+)`)
)

// ParsedOutput is the structured result of parsing an LLM response.
type ParsedOutput struct {
	PipelineYAML string
	GoFiles      map[string]string // filename → Go source
}

// Parse extracts the YAML config and Go transform files from a raw LLM response.
// Returns an error if the response doesn't contain exactly one YAML block,
// or if a Go block is missing the required // transform: <name> header.
func Parse(raw string) (*ParsedOutput, error) {
	// --- YAML block ---
	yamlMatches := yamlBlockRe.FindAllStringSubmatch(raw, -1)
	if len(yamlMatches) == 0 {
		return nil, fmt.Errorf("response contains no ```yaml block")
	}
	if len(yamlMatches) > 1 {
		return nil, fmt.Errorf("response contains %d ```yaml blocks, expected exactly 1", len(yamlMatches))
	}
	pipelineYAML := strings.TrimSpace(yamlMatches[0][1])

	// --- Go blocks ---
	goFiles := make(map[string]string)
	goMatches := goBlockRe.FindAllStringSubmatch(raw, -1)
	for _, match := range goMatches {
		src := strings.TrimSpace(match[1])
		// first line must be: // transform: <name>
		firstLine := strings.SplitN(src, "\n", 2)[0]
		nameMatch := transformNameRe.FindStringSubmatch(firstLine)
		if nameMatch == nil {
			return nil, fmt.Errorf(
				"Go block is missing required header comment\n"+
					"  expected: // transform: <name>\n"+
					"  got:      %s", firstLine)
		}
		name := nameMatch[1]
		filename := name + ".go"
		goFiles[filename] = src
	}

	return &ParsedOutput{
		PipelineYAML: pipelineYAML,
		GoFiles:      goFiles,
	}, nil
}
