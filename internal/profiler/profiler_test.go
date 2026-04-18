package profiler

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/higino/eulantir/internal/connector"
)

// ── helpers ────────────────────────────────────────────────────────────────

func makeRecord(row map[string]any) connector.Record {
	b, _ := json.Marshal(row)
	return connector.Record{Value: b}
}

func makeRecords(rows []map[string]any) []connector.Record {
	recs := make([]connector.Record, len(rows))
	for i, r := range rows {
		recs[i] = makeRecord(r)
	}
	return recs
}

// mockSource emits a fixed set of records then returns io.EOF.
type mockSource struct {
	records []connector.Record
	pos     int
}

func (m *mockSource) Name() string                                     { return "mock" }
func (m *mockSource) Open(_ context.Context, _ map[string]any) error   { return nil }
func (m *mockSource) Close(_ context.Context) error                    { return nil }
func (m *mockSource) Commit(_ context.Context, _ int64) error          { return nil }
func (m *mockSource) ReadBatch(_ context.Context, maxSize int) ([]connector.Record, error) {
	if m.pos >= len(m.records) {
		return nil, io.EOF
	}
	end := m.pos + maxSize
	if end > len(m.records) {
		end = len(m.records)
	}
	batch := m.records[m.pos:end]
	m.pos = end
	if m.pos >= len(m.records) {
		return batch, io.EOF
	}
	return batch, nil
}

// ── Profile tests ──────────────────────────────────────────────────────────

func TestProfile_StringField(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"name": "Alice"},
		{"name": "Bob"},
		{"name": "Carol"},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Fields) != 1 {
		t.Fatalf("expected 1 field, got %d", len(p.Fields))
	}
	f := p.Fields[0]
	if f.Name != "name" {
		t.Errorf("expected field name 'name', got %q", f.Name)
	}
	if f.Type != TypeString {
		t.Errorf("expected TypeString, got %q", f.Type)
	}
	if f.NullRate != 0 {
		t.Errorf("expected 0 null rate, got %f", f.NullRate)
	}
	if f.Cardinality != 3 {
		t.Errorf("expected cardinality 3, got %d", f.Cardinality)
	}
}

func TestProfile_NumberField(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"amount": 10.0},
		{"amount": 20.0},
		{"amount": 30.0},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	f := p.Fields[0]
	if f.Type != TypeNumber {
		t.Errorf("expected TypeNumber, got %q", f.Type)
	}
	if f.Min == nil || *f.Min != 10.0 {
		t.Errorf("expected min=10, got %v", f.Min)
	}
	if f.Max == nil || *f.Max != 30.0 {
		t.Errorf("expected max=30, got %v", f.Max)
	}
	if f.Mean == nil || *f.Mean != 20.0 {
		t.Errorf("expected mean=20, got %v", f.Mean)
	}
}

func TestProfile_BooleanField(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"active": true},
		{"active": false},
		{"active": true},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	if p.Fields[0].Type != TypeBoolean {
		t.Errorf("expected TypeBoolean, got %q", p.Fields[0].Type)
	}
}

func TestProfile_NullRate(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"email": "a@b.com"},
		{"email": nil},
		{"email": "c@d.com"},
		{"email": nil},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	f := p.Fields[0]
	if f.NullRate != 0.5 {
		t.Errorf("expected null rate 0.5, got %f", f.NullRate)
	}
}

func TestProfile_MixedType(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"x": "hello"},
		{"x": 42.0},
		{"x": "world"},
		{"x": 99.0},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	// 2 strings, 2 numbers — neither is ≥80% — should be mixed
	if p.Fields[0].Type != TypeMixed {
		t.Errorf("expected TypeMixed, got %q", p.Fields[0].Type)
	}
}

func TestProfile_PatternEmail(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"contact": "alice@example.com"},
		{"contact": "bob@example.org"},
		{"contact": "carol@test.io"},
		{"contact": "dave@work.net"},
		{"contact": "eve@foo.com"},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	if p.Fields[0].Pattern != "email" {
		t.Errorf("expected pattern 'email', got %q", p.Fields[0].Pattern)
	}
}

func TestProfile_PatternUUID(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"id": "550e8400-e29b-41d4-a716-446655440000"},
		{"id": "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"id": "6ba7b811-9dad-11d1-80b4-00c04fd430c8"},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	if p.Fields[0].Pattern != "uuid" {
		t.Errorf("expected pattern 'uuid', got %q", p.Fields[0].Pattern)
	}
}

func TestProfile_PatternDate(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"created": "2024-01-15"},
		{"created": "2024-02-20"},
		{"created": "2024-03-01"},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	if p.Fields[0].Pattern != "date" {
		t.Errorf("expected pattern 'date', got %q", p.Fields[0].Pattern)
	}
}

func TestProfile_NoPatternBelowThreshold(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"val": "alice@example.com"},
		{"val": "not-an-email"},
		{"val": "also-not-an-email"},
		{"val": "still-not"},
		{"val": "nope"},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	// Only 1/5 = 20% match email — below 80% threshold
	if p.Fields[0].Pattern != "" {
		t.Errorf("expected no pattern, got %q", p.Fields[0].Pattern)
	}
}

func TestProfile_MultipleFields(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"id": 1.0, "name": "Alice", "active": true},
		{"id": 2.0, "name": "Bob", "active": false},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Fields) != 3 {
		t.Fatalf("expected 3 fields, got %d", len(p.Fields))
	}
	// Fields are sorted alphabetically: active, id, name
	if p.Fields[0].Name != "active" {
		t.Errorf("expected first field 'active', got %q", p.Fields[0].Name)
	}
}

func TestProfile_EmptyRecords(t *testing.T) {
	p, err := Profile("test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Fields) != 0 {
		t.Errorf("expected 0 fields for empty records, got %d", len(p.Fields))
	}
	if p.TotalSeen != 0 {
		t.Errorf("expected TotalSeen=0, got %d", p.TotalSeen)
	}
}

func TestProfile_StringLengths(t *testing.T) {
	records := makeRecords([]map[string]any{
		{"code": "A"},
		{"code": "AB"},
		{"code": "ABC"},
	})
	p, err := Profile("test", records)
	if err != nil {
		t.Fatal(err)
	}
	f := p.Fields[0]
	if f.MinLen == nil || *f.MinLen != 1 {
		t.Errorf("expected min_len=1, got %v", f.MinLen)
	}
	if f.MaxLen == nil || *f.MaxLen != 3 {
		t.Errorf("expected max_len=3, got %v", f.MaxLen)
	}
}

// ── Sample tests ───────────────────────────────────────────────────────────

func TestSample_ReturnsAllRecords(t *testing.T) {
	src := &mockSource{records: makeRecords([]map[string]any{
		{"x": 1.0}, {"x": 2.0}, {"x": 3.0},
	})}
	records, err := Sample(context.Background(), src, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 3 {
		t.Errorf("expected 3 records, got %d", len(records))
	}
}

func TestSample_RespectsLimit(t *testing.T) {
	rows := make([]map[string]any, 500)
	for i := range rows {
		rows[i] = map[string]any{"i": float64(i)}
	}
	src := &mockSource{records: makeRecords(rows)}
	records, err := Sample(context.Background(), src, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) > 100 {
		t.Errorf("expected ≤100 records, got %d", len(records))
	}
}

func TestSample_EmptySource(t *testing.T) {
	src := &mockSource{records: nil}
	records, err := Sample(context.Background(), src, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Errorf("expected 0 records, got %d", len(records))
	}
}
