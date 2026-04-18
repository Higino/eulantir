package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var validateCmd = &cobra.Command{
	Use:   "validate <pipeline.yaml>",
	Short: "Validate a pipeline config file without running it",
	Long: `Validate parses the pipeline YAML, checks it against the schema,
resolves all connector and transform references, and verifies the
task DAG has no cycles.

Exits 0 on success, 1 on any validation error.

Example:
  eulantir validate my-pipeline/pipeline.yaml`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		if err := validatePipeline(args[0]); err != nil {
			return fmt.Errorf("config error: %w", err)
		}
		return nil
	},
}
