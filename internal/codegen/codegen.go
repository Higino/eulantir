package codegen

// GeneratedPipeline is written to disk after the agent produces valid output.
// Full implementation comes in Phase 5.
type GeneratedPipeline struct {
	PipelineYAML string
	GoFiles      map[string]string // filename → source code
}
