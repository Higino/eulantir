package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/dlq"
	"github.com/higino/eulantir/internal/engine"
	"github.com/higino/eulantir/internal/lineage"
	"github.com/higino/eulantir/internal/visualize"

	_ "github.com/higino/eulantir/internal/connectors/csv"
	_ "github.com/higino/eulantir/internal/connectors/kafka"
	_ "github.com/higino/eulantir/internal/connectors/postgres"
	_ "github.com/higino/eulantir/internal/connectors/s3"

	"github.com/spf13/cobra"
)

var serveCmd = &cobra.Command{
	Use:   "serve <pipeline.yaml>",
	Short: "Start a live visualization dashboard for a pipeline",
	Long: `Serve loads the pipeline config and starts an HTTP server that renders
an interactive graph of all tasks and their dependencies.

Use --run to also execute the pipeline: task nodes update in real-time
as each task completes (success / failed / skipped).

Examples:
  eulantir serve pipeline.yaml              # view pipeline structure
  eulantir serve pipeline.yaml --run        # view + execute
  eulantir serve pipeline.yaml --port 9090  # custom port`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		port, _ := cmd.Flags().GetInt("port")
		run, _ := cmd.Flags().GetBool("run")
		verbose, _ := cmd.Flags().GetBool("verbose")
		configureLogging(verbose)

		cfg, err := config.Load(args[0])
		if err != nil {
			return fmt.Errorf("config error: %w", err)
		}

		graph := visualize.Build(cfg)
		srv := visualize.NewServer(graph)

		addr := fmt.Sprintf(":%d", port)
		httpSrv := &http.Server{Addr: addr, Handler: srv}

		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		if run {
			go func() {
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
					slog.Error("engine error", "err", err)
					return
				}
				for result := range results {
					srv.Push(result)
				}
				srv.Done()
			}()
		}

		go func() {
			if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				slog.Error("http server error", "err", err)
			}
		}()

		fmt.Fprintf(os.Stderr, "▶  Dashboard at http://localhost:%d", port)
		if run {
			fmt.Fprintf(os.Stderr, "  (pipeline executing)\n")
		} else {
			fmt.Fprintf(os.Stderr, "\n")
		}
		fmt.Fprintln(os.Stderr, "   Press Ctrl+C to stop")

		<-ctx.Done()
		fmt.Fprintln(os.Stderr, "\n⏹  Shutting down")
		return httpSrv.Shutdown(context.Background())
	},
}

func init() {
	serveCmd.Flags().IntP("port", "p", 8080, "port to listen on")
	serveCmd.Flags().Bool("run", false, "execute the pipeline while serving the dashboard")
}
