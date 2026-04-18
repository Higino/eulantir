package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "eulantir",
	Short: "A generic data pipeline orchestrator with LLM-powered code generation",
	Long: `Eulantir is a data pipeline tool that lets you describe what you want
in plain English, generates the pipeline config and transform code via an LLM,
and then executes the pipeline.

Examples:
  eulantir generate "read CSV from S3, clean nulls, write to Postgres"
  eulantir run pipeline.yaml
  eulantir validate pipeline.yaml
  eulantir connectors list`,
}

// Execute is the entrypoint called by main.go.
func Execute() {
	// cobra already prints "Error: <msg>" — we just set the exit code
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "global config file (default: $HOME/.eulantir.yaml)")
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "enable verbose logging")

	rootCmd.AddCommand(runCmd)
	rootCmd.AddCommand(generateCmd)
	rootCmd.AddCommand(validateCmd)
	rootCmd.AddCommand(connectorsCmd)
	rootCmd.AddCommand(visualizeCmd)
	rootCmd.AddCommand(serveCmd)
}
