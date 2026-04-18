package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/dlq"
	"github.com/higino/eulantir/internal/engine"
	"github.com/higino/eulantir/internal/lineage"

	// import built-in connectors so their init() functions register them
	_ "github.com/higino/eulantir/internal/connectors/csv"
	_ "github.com/higino/eulantir/internal/connectors/kafka"
	_ "github.com/higino/eulantir/internal/connectors/postgres"
	_ "github.com/higino/eulantir/internal/connectors/s3"

	"github.com/spf13/cobra"
)

var runCmd = &cobra.Command{
	Use:   "run <pipeline.yaml>",
	Short: "Execute a pipeline from a config file",
	Long: `Run loads the pipeline config, validates it, builds the task DAG,
and executes every task in dependency order.

Progress and record counts are printed as each task completes.

Example:
  eulantir run my-pipeline/pipeline.yaml`,
	Args: cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		cmd.SilenceUsage = true
		verbose, _ := cmd.Flags().GetBool("verbose")
		configureLogging(verbose)
		return runPipeline(cmd.Context(), args[0])
	},
}

// runPipeline loads, validates, and executes a pipeline from the given YAML path.
// It is called by both the `run` command and the `generate --run` flag.
func runPipeline(ctx context.Context, pipelinePath string) error {
	cfg, err := config.Load(pipelinePath)
	if err != nil {
		return fmt.Errorf("config error: %w", err)
	}
	fmt.Fprintf(os.Stderr, "▶  Running pipeline %q (%d tasks)\n\n", cfg.Name, len(cfg.Tasks))

	// set up DLQ if enabled
	var pipelineDLQ dlq.DLQ
	if cfg.DLQ.Enabled {
		dir := "./dlq"
		if p, ok := cfg.DLQ.Config["path"].(string); ok && p != "" {
			dir = p
		}
		pipelineDLQ = dlq.NewFileDLQ(dir)
	}

	eng := &engine.LocalEngine{
		Registry: connector.Default,
		DLQ:      pipelineDLQ,
		Lineage:  lineage.New(cfg.Lineage),
	}

	results, err := eng.Run(ctx, *cfg)
	if err != nil {
		return fmt.Errorf("engine error: %w", err)
	}

	var failed int
	for result := range results {
		icon := "✔"
		if result.Status == engine.StatusFailed {
			icon = "✘"
			failed++
		} else if result.Status == engine.StatusSkipped {
			icon = "⊘"
		}
		duration := result.FinishedAt.Sub(result.StartedAt).Round(time.Millisecond)

		if result.Err != nil {
			fmt.Fprintf(os.Stderr, "  %s  %-20s  %s\n", icon, result.NodeID, result.Err)
		} else {
			fmt.Fprintf(os.Stderr, "  %s  %-20s  in:%-8d out:%-8d dlq:%-4d  %s\n",
				icon, result.NodeID,
				result.RecordsIn, result.RecordsOut, result.RecordsDLQ,
				duration)
		}
	}

	fmt.Fprintln(os.Stderr)
	if failed > 0 {
		return fmt.Errorf("%d task(s) failed", failed)
	}
	fmt.Fprintf(os.Stderr, "✅  Pipeline %q finished successfully\n", cfg.Name)
	return nil
}

// configureLogging sets the global slog level.
func configureLogging(verbose bool) {
	level := slog.LevelInfo
	if verbose {
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
}
