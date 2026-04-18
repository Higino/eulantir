package cmd

import (
	"fmt"
	"os"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/visualize"
	"github.com/spf13/cobra"
)

var visualizeCmd = &cobra.Command{
	Use:   "visualize <pipeline.yaml>",
	Short: "Export a pipeline DAG as a self-contained HTML file",
	Long: `Visualize loads the pipeline config and renders an interactive graph
of all tasks, their dependencies, and connector types.

The output is a single .html file that can be opened in any browser.
No server required — just open the file.

Example:
  eulantir visualize pipeline.yaml
  eulantir visualize pipeline.yaml -o graph.html`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		output, _ := cmd.Flags().GetString("output")

		cfg, err := config.Load(args[0])
		if err != nil {
			return fmt.Errorf("config error: %w", err)
		}

		graph := visualize.Build(cfg)
		html, err := visualize.RenderHTML(graph, false)
		if err != nil {
			return fmt.Errorf("render error: %w", err)
		}

		if output == "" {
			output = cfg.Name + ".html"
		}
		if err := os.WriteFile(output, html, 0o644); err != nil {
			return err
		}

		fmt.Fprintf(os.Stderr, "✔  Pipeline graph written to %s\n", output)
		return nil
	},
}

func init() {
	visualizeCmd.Flags().StringP("output", "o", "", "output HTML file (default: <pipeline-name>.html)")
}
