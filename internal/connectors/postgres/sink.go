package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/higino/eulantir/internal/connector"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Sink writes records to a PostgreSQL table using an upsert strategy.
// Records' Value must be a JSON-encoded object. Column names are derived
// from the JSON keys. The upsert_key config field defines the conflict target
// for idempotent at-least-once writes.
type Sink struct {
	dsn       string
	table     string
	upsertKey []string
	pool      *pgxpool.Pool
}

func (s *Sink) Name() string { return "postgres" }

func (s *Sink) Open(ctx context.Context, cfg map[string]any) error {
	s.dsn = stringVal(cfg, "dsn", "")
	if s.dsn == "" {
		return fmt.Errorf("postgres sink: missing required config key \"dsn\"")
	}
	s.table = stringVal(cfg, "table", "")
	if s.table == "" {
		return fmt.Errorf("postgres sink: missing required config key \"table\"")
	}

	// upsert_key can be a []any (from YAML) or a []string
	if raw, ok := cfg["upsert_key"]; ok {
		switch v := raw.(type) {
		case []any:
			for _, item := range v {
				if str, ok := item.(string); ok {
					s.upsertKey = append(s.upsertKey, str)
				}
			}
		case []string:
			s.upsertKey = v
		}
	}

	pool, err := pgxpool.New(ctx, s.dsn)
	if err != nil {
		return fmt.Errorf("postgres sink: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("postgres sink: ping: %w", err)
	}
	s.pool = pool
	return nil
}

func (s *Sink) Close(_ context.Context) error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// WriteBatch upserts a batch of records using pgx's batch API.
// Each record's Value is unmarshalled as a JSON object; keys become columns.
// When upsert_key is set the query uses ON CONFLICT DO UPDATE SET for
// idempotent writes. Without upsert_key it falls back to plain INSERT.
func (s *Sink) WriteBatch(ctx context.Context, records []connector.Record) error {
	if len(records) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, r := range records {
		var row map[string]any
		if err := json.Unmarshal(r.Value, &row); err != nil {
			return fmt.Errorf("postgres sink: unmarshal record: %w", err)
		}

		query, args, err := s.buildUpsert(row)
		if err != nil {
			return err
		}
		batch.Queue(query, args...)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range records {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("postgres sink: exec batch: %w", err)
		}
	}
	return nil
}

// buildUpsert constructs an INSERT ... ON CONFLICT DO UPDATE SET ... query.
func (s *Sink) buildUpsert(row map[string]any) (string, []any, error) {
	// sort columns for deterministic query shape
	cols := make([]string, 0, len(row))
	for k := range row {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	placeholders := make([]string, len(cols))
	args := make([]any, len(cols))
	for i, col := range cols {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
		args[i] = row[col]
	}

	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = pgx.Identifier{c}.Sanitize()
	}

	query := fmt.Sprintf(
		"INSERT INTO %s (%s) VALUES (%s)",
		pgx.Identifier(strings.Split(s.table, ".")).Sanitize(),
		strings.Join(quotedCols, ", "),
		strings.Join(placeholders, ", "),
	)

	if len(s.upsertKey) > 0 {
		conflictCols := make([]string, len(s.upsertKey))
		for i, k := range s.upsertKey {
			conflictCols[i] = pgx.Identifier{k}.Sanitize()
		}

		setClauses := []string{}
		for _, col := range quotedCols {
			isKey := false
			for _, k := range conflictCols {
				if k == col {
					isKey = true
					break
				}
			}
			if !isKey {
				setClauses = append(setClauses, fmt.Sprintf("%s = EXCLUDED.%s", col, col))
			}
		}

		query += fmt.Sprintf(" ON CONFLICT (%s)", strings.Join(conflictCols, ", "))
		if len(setClauses) > 0 {
			query += fmt.Sprintf(" DO UPDATE SET %s", strings.Join(setClauses, ", "))
		} else {
			query += " DO NOTHING"
		}
	}

	return query, args, nil
}

// helpers

func stringVal(cfg map[string]any, key, def string) string {
	if v, ok := cfg[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return def
}

func init() {
	connector.Default.RegisterSink(connector.ConnectorInfo{
		Type:        "postgres",
		Description: "Write records to a PostgreSQL table (upsert)",
		ConfigKeys:  []string{"dsn", "table", "upsert_key"},
	}, func(ctx context.Context, cfg map[string]any) (connector.Sink, error) {
		s := &Sink{}
		if err := s.Open(ctx, cfg); err != nil {
			return nil, err
		}
		return s, nil
	})
}
