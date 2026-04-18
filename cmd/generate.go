package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/higino/eulantir/internal/agent"
	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/dag"
	"github.com/higino/eulantir/internal/llm"

	// register connectors so the live catalog includes all built-in types
	_ "github.com/higino/eulantir/internal/connectors/csv"
	_ "github.com/higino/eulantir/internal/connectors/kafka"
	_ "github.com/higino/eulantir/internal/connectors/postgres"
	_ "github.com/higino/eulantir/internal/connectors/s3"

	"github.com/spf13/cobra"
)

var (
	generateOutput   string
	generateProvider string
	generateModel    string
	generateBaseURL  string
	generateValidate bool
	generateRun      bool
)

var generateCmd = &cobra.Command{
	Use:   "generate <intent>",
	Short: "Generate a pipeline config and transform code from a plain-English description",
	Long: `Generate uses an LLM to turn your intent into a ready-to-run pipeline.

It produces:
  - pipeline.yaml   the pipeline definition (tasks, connectors, transforms)
  - generated/*.go  transform logic written by the LLM

Flags --validate and --run let you immediately validate or execute the pipeline
after generation without separate commands.

Examples:
  eulantir generate "read customer CSV, filter where status is active, write to Postgres"
  eulantir generate --provider openai --model gpt-4o "aggregate logs by service"
  eulantir generate --output ./my-pipeline --validate --run "read orders CSV, write to CSV"`,
	Args: cobra.MinimumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		intent := strings.Join(args, " ")
		verbose, _ := cmd.Flags().GetBool("verbose")
		configureLogging(verbose)

		fmt.Fprintf(os.Stderr, "🤖 Generating pipeline...\n")
		fmt.Fprintf(os.Stderr, "   Provider : %s\n", generateProvider)
		fmt.Fprintf(os.Stderr, "   Model    : %s\n", generateModel)
		fmt.Fprintf(os.Stderr, "   Intent   : %s\n\n", intent)

		// build LLM provider
		provider, err := llm.BuildProvider(llm.ProviderConfig{
			Type:    generateProvider,
			BaseURL: generateBaseURL,
			Model:   generateModel,
		})
		if err != nil {
			return fmt.Errorf("build provider: %w", err)
		}

		// run agent with the live connector catalog
		a := agent.New(provider, generateModel, connector.Default.List())
		result, err := a.Generate(cmd.Context(), intent)
		if err != nil {
			return fmt.Errorf("generation failed: %w", err)
		}

		// write pipeline.yaml
		if err := os.MkdirAll(generateOutput, 0o755); err != nil {
			return fmt.Errorf("create output dir: %w", err)
		}
		pipelinePath := filepath.Join(generateOutput, "pipeline.yaml")
		if err := os.WriteFile(pipelinePath, []byte(result.PipelineYAML), 0o644); err != nil {
			return fmt.Errorf("write pipeline.yaml: %w", err)
		}
		fmt.Fprintf(os.Stderr, "✔  Written: %s\n", pipelinePath)

		// write generated/*.go transform files
		if len(result.GoFiles) > 0 {
			genDir := filepath.Join(generateOutput, "generated")
			if err := os.MkdirAll(genDir, 0o755); err != nil {
				return fmt.Errorf("create generated dir: %w", err)
			}
			for filename, src := range result.GoFiles {
				outPath := filepath.Join(genDir, filename)
				if err := os.WriteFile(outPath, []byte(src), 0o644); err != nil {
					return fmt.Errorf("write %s: %w", filename, err)
				}
				fmt.Fprintf(os.Stderr, "✔  Written: %s\n", outPath)
			}
		}
		fmt.Fprintln(os.Stderr)

		// print pipeline.yaml to stdout for easy inspection / piping
		fmt.Println(result.PipelineYAML)

		// --validate: run structural validation and print execution order
		if generateValidate || generateRun {
			fmt.Fprintln(os.Stderr)
			if err := validatePipeline(pipelinePath); err != nil {
				return fmt.Errorf("validation failed: %w", err)
			}
		}

		// --run: execute the pipeline immediately
		if generateRun {
			fmt.Fprintln(os.Stderr)
			if err := runPipeline(cmd.Context(), pipelinePath); err != nil {
				return fmt.Errorf("run failed: %w", err)
			}
		}

		if !generateValidate && !generateRun {
			fmt.Fprintf(os.Stderr, "✅ Done. Next steps:\n")
			fmt.Fprintf(os.Stderr, "   eulantir validate %s\n", pipelinePath)
			fmt.Fprintf(os.Stderr, "   eulantir run      %s\n", pipelinePath)
		}

		return nil
	},
}

// validatePipeline loads the config, builds the DAG, and prints execution order.
// Shared between the `validate` command and `generate --validate`.
func validatePipeline(pipelinePath string) error {
	fmt.Fprintf(os.Stderr, "Validating %s\n\n", pipelinePath)

	cfg, err := config.Load(pipelinePath)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "  ✔  schema valid\n")
	fmt.Fprintf(os.Stderr, "  ✔  %d connector(s), %d transform(s), %d task(s)\n",
		len(cfg.Connectors), len(cfg.Transforms), len(cfg.Tasks))

	_, sorted, err := dag.Build(cfg.Tasks)
	if err != nil {
		return fmt.Errorf("DAG error: %w", err)
	}
	fmt.Fprintf(os.Stderr, "  ✔  DAG is acyclic\n\n")
	fmt.Fprintf(os.Stderr, "Execution order:\n")
	for i, node := range sorted {
		label := buildNodeLabel(node.ConnectorRef, node.TransformRef)
		fmt.Fprintf(os.Stderr, "  %d. %-10s [%s]\n", i+1, node.ID, label)
	}
	fmt.Fprintln(os.Stderr)
	fmt.Fprintf(os.Stderr, "✅  %s is valid\n", cfg.Name)
	return nil
}

func init() {
	generateCmd.Flags().StringVarP(&generateOutput, "output", "o", ".", "output directory for generated files")
	generateCmd.Flags().StringVar(&generateProvider, "provider", "ollama", "LLM provider: ollama | openai | anthropic")
	generateCmd.Flags().StringVar(&generateModel, "model", "llama3.2", "model name to use for generation")
	generateCmd.Flags().StringVar(&generateBaseURL, "base-url", "", "override LLM base URL")
	generateCmd.Flags().BoolVar(&generateValidate, "validate", false, "validate the pipeline immediately after generation")
	generateCmd.Flags().BoolVar(&generateRun, "run", false, "validate and run the pipeline immediately after generation")
}
