package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/higino/eulantir/internal/catalog"
	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/llm"
	"github.com/higino/eulantir/internal/profiler"

	_ "github.com/higino/eulantir/internal/connectors/csv"
	_ "github.com/higino/eulantir/internal/connectors/kafka"
	_ "github.com/higino/eulantir/internal/connectors/postgres"
	_ "github.com/higino/eulantir/internal/connectors/s3"
)

var catalogCmd = &cobra.Command{
	Use:   "catalog",
	Short: "Automatic data cataloging powered by statistical profiling and LLM annotation",
}

// ── catalog profile ────────────────────────────────────────────────────────

var catalogProfileCmd = &cobra.Command{
	Use:   "profile <pipeline.yaml>",
	Short: "Sample source connectors and output statistical field profiles",
	Args:  cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		sampleSize, _ := cmd.Flags().GetInt("sample")
		outputFile, _ := cmd.Flags().GetString("output")

		cfg, err := config.Load(args[0])
		if err != nil {
			return err
		}

		ctx := context.Background()
		profiles, err := profileSources(ctx, cfg, sampleSize)
		if err != nil {
			return err
		}

		return writeOutput(profiles, outputFile, func() {
			for _, p := range profiles {
				printProfile(p)
			}
		})
	},
}

// ── catalog enrich ─────────────────────────────────────────────────────────

var catalogEnrichCmd = &cobra.Command{
	Use:   "enrich <pipeline.yaml>",
	Short: "Profile source connectors and enrich with LLM semantic annotations",
	Args:  cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		sampleSize, _ := cmd.Flags().GetInt("sample")
		outputFile, _ := cmd.Flags().GetString("output")
		provider, _ := cmd.Flags().GetString("provider")
		model, _ := cmd.Flags().GetString("model")
		apiKey, _ := cmd.Flags().GetString("api-key")
		baseURL, _ := cmd.Flags().GetString("base-url")

		cfg, err := config.Load(args[0])
		if err != nil {
			return err
		}

		prov, err := llm.BuildProvider(llm.ProviderConfig{
			Type:    provider,
			Model:   model,
			APIKey:  apiKey,
			BaseURL: baseURL,
		})
		if err != nil {
			return fmt.Errorf("build LLM provider: %w", err)
		}

		ctx := context.Background()
		profiles, err := profileSources(ctx, cfg, sampleSize)
		if err != nil {
			return err
		}

		enricher := catalog.New(prov, model)
		var enriched []*catalog.EnrichedProfile
		for _, p := range profiles {
			fmt.Fprintf(os.Stderr, "Enriching %s...\n", p.Source)
			ep, err := enricher.Enrich(ctx, p)
			if err != nil {
				fmt.Fprintf(os.Stderr, "  warning: enrichment failed for %s: %v\n", p.Source, err)
				continue
			}
			enriched = append(enriched, ep)
		}

		return writeOutput(enriched, outputFile, func() {
			for _, ep := range enriched {
				printEnriched(ep)
			}
		})
	},
}

// ── helpers ────────────────────────────────────────────────────────────────

// profileSources iterates all source connectors in the pipeline config,
// opens each one, samples records, and returns a profile per source.
func profileSources(ctx context.Context, cfg *config.PipelineConfig, sampleSize int) ([]*profiler.DatasetProfile, error) {
	var profiles []*profiler.DatasetProfile

	for _, cc := range cfg.Connectors {
		if connector.Default.IsSink(cc.Type) {
			continue
		}

		fmt.Fprintf(os.Stderr, "Profiling %s (%s)...\n", cc.Name, cc.Type)

		src, err := connector.Default.BuildSource(cc.Type, cc.Config)
		if err != nil {
			return nil, fmt.Errorf("open source %q: %w", cc.Name, err)
		}
		defer src.Close(ctx) //nolint:errcheck

		records, err := profiler.Sample(ctx, src, sampleSize)
		if err != nil {
			return nil, fmt.Errorf("sample source %q: %w", cc.Name, err)
		}

		if len(records) == 0 {
			fmt.Fprintf(os.Stderr, "  warning: no records returned from %s\n", cc.Name)
			continue
		}
		fmt.Fprintf(os.Stderr, "  sampled %d records\n", len(records))

		p, err := profiler.Profile(cc.Name, records)
		if err != nil {
			return nil, fmt.Errorf("profile source %q: %w", cc.Name, err)
		}
		profiles = append(profiles, p)
	}

	return profiles, nil
}

// writeOutput marshals v to JSON in outputFile if given, otherwise calls printFn.
func writeOutput(v any, outputFile string, printFn func()) error {
	if outputFile == "" {
		printFn()
		return nil
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(outputFile, data, 0o644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "✔  Written: %s\n", outputFile)
	return nil
}

// printProfile prints a human-readable statistical summary to stdout.
func printProfile(p *profiler.DatasetProfile) {
	fmt.Printf("\nDataset: %s  (%d records sampled)\n", p.Source, p.TotalSeen)
	fmt.Printf("%-24s  %-10s  %-8s  %-12s  %s\n", "FIELD", "TYPE", "NULL%", "CARDINALITY", "NOTES")
	fmt.Printf("%s\n", strings.Repeat("-", 80))
	for _, f := range p.Fields {
		notes := buildNotes(f)
		fmt.Printf("%-24s  %-10s  %-8s  %-12d  %s\n",
			truncate(f.Name, 24),
			f.Type,
			fmt.Sprintf("%.1f%%", f.NullRate*100),
			f.Cardinality,
			notes,
		)
	}
}

// printEnriched prints a human-readable enriched catalog entry to stdout.
func printEnriched(ep *catalog.EnrichedProfile) {
	fmt.Printf("\n══════════════════════════════════════════════════════════\n")
	fmt.Printf("Dataset : %s\n", ep.Profile.Source)
	fmt.Printf("Domain  : %s\n", ep.Domain)
	fmt.Printf("Desc    : %s\n", ep.DatasetDescription)
	fmt.Printf("Model   : %s\n", ep.Model)
	fmt.Printf("──────────────────────────────────────────────────────────\n")
	fmt.Printf("%-20s  %-14s  %-5s  %s\n", "FIELD", "SEMANTIC TYPE", "PII", "DESCRIPTION")
	fmt.Printf("%s\n", strings.Repeat("-", 80))

	byName := map[string]catalog.FieldAnnotation{}
	for _, a := range ep.Annotations {
		byName[a.Name] = a
	}

	for _, f := range ep.Profile.Fields {
		a, ok := byName[f.Name]
		if !ok {
			a = catalog.FieldAnnotation{Name: f.Name, SemanticType: "unknown"}
		}
		piiFlag := " "
		if a.PII {
			piiFlag = "✓"
		}
		fmt.Printf("%-20s  %-14s  %-5s  %s\n",
			truncate(f.Name, 20),
			truncate(a.SemanticType, 14),
			piiFlag,
			a.Description,
		)
	}
}

func buildNotes(f profiler.FieldProfile) string {
	var parts []string
	if f.Pattern != "" {
		parts = append(parts, "pattern:"+f.Pattern)
	}
	if f.Min != nil && f.Max != nil {
		parts = append(parts, fmt.Sprintf("range:[%.4g, %.4g]", *f.Min, *f.Max))
	}
	if f.MinLen != nil && f.MaxLen != nil {
		parts = append(parts, fmt.Sprintf("len:%d-%d", *f.MinLen, *f.MaxLen))
	}
	if len(f.Examples) > 0 {
		parts = append(parts, fmt.Sprintf("ex:%s", strings.Join(f.Examples[:min(2, len(f.Examples))], ",")))
	}
	return strings.Join(parts, "  ")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func init() {
	// profile flags
	catalogProfileCmd.Flags().Int("sample", 1000, "maximum number of records to sample per source")
	catalogProfileCmd.Flags().String("output", "", "write JSON output to file (default: print to stdout)")

	// enrich flags
	catalogEnrichCmd.Flags().Int("sample", 1000, "maximum number of records to sample per source")
	catalogEnrichCmd.Flags().String("output", "", "write JSON output to file (default: print to stdout)")
	catalogEnrichCmd.Flags().String("provider", "ollama", "LLM provider: ollama | openai | anthropic")
	catalogEnrichCmd.Flags().String("model", "llama3.2", "LLM model name")
	catalogEnrichCmd.Flags().String("api-key", "", "LLM API key (or set $OPENAI_API_KEY / $ANTHROPIC_API_KEY)")
	catalogEnrichCmd.Flags().String("base-url", "", "override LLM endpoint URL")

	catalogCmd.AddCommand(catalogProfileCmd)
	catalogCmd.AddCommand(catalogEnrichCmd)
}
