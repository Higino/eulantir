package transform_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/transform"
)

const (
	filterSrc      = "../../testdata/transforms/filter_active.go"
	uppercaseSrc   = "../../testdata/transforms/uppercase_name.go"
	countryLookSrc = "../../testdata/transforms/country_lookup.go"
)

func newLoader(t *testing.T) *transform.Loader {
	t.Helper()
	l := &transform.Loader{CacheDir: t.TempDir()}
	// When running under go test -cover the test binary is compiled with
	// coverage instrumentation. The plugin must be compiled the same way,
	// otherwise Go's version checker rejects it with "different version of
	// package". Passing -cover to the plugin build fixes this.
	if testing.CoverMode() != "" {
		l.ExtraBuildFlags = []string{"-cover"}
	}
	return l
}

func makeRecord(t *testing.T, fields map[string]any) connector.Record {
	t.Helper()
	b, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return connector.Record{Value: b}
}

// TestLoad_FilterActive_PassThrough verifies that an "active" record is kept.
func TestLoad_FilterActive_PassThrough(t *testing.T) {
	l := newLoader(t)
	tr, err := l.Load("filter-active", filterSrc, "FilterActive")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	rec := makeRecord(t, map[string]any{"id": "1", "name": "Alice", "status": "active"})
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("expected 1 output record for active status, got %d", len(out))
	}
}

// TestLoad_FilterActive_Drop verifies that a non-active record is dropped.
func TestLoad_FilterActive_Drop(t *testing.T) {
	l := newLoader(t)
	tr, err := l.Load("filter-active", filterSrc, "FilterActive")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	rec := makeRecord(t, map[string]any{"id": "2", "name": "Bob", "status": "inactive"})
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected 0 output records for inactive status, got %d", len(out))
	}
}

// TestLoad_UppercaseName verifies that the mapper rewrites the name field.
func TestLoad_UppercaseName(t *testing.T) {
	l := newLoader(t)
	tr, err := l.Load("uppercase-name", uppercaseSrc, "UppercaseName")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	rec := makeRecord(t, map[string]any{"id": "1", "name": "alice"})
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output record, got %d", len(out))
	}

	var row map[string]any
	if err := json.Unmarshal(out[0].Value, &row); err != nil {
		t.Fatalf("unmarshal output: %v", err)
	}
	if row["name"] != "ALICE" {
		t.Errorf("expected name=ALICE, got %q", row["name"])
	}
}

// TestLoad_CacheHit verifies that loading the same transform twice
// produces two working transforms (second load uses the cached .so).
func TestLoad_CacheHit(t *testing.T) {
	l := newLoader(t)

	tr1, err := l.Load("filter-active", filterSrc, "FilterActive")
	if err != nil {
		t.Fatalf("first Load: %v", err)
	}
	tr2, err := l.Load("filter-active", filterSrc, "FilterActive")
	if err != nil {
		t.Fatalf("second Load (cache hit): %v", err)
	}
	if tr1.Name() != tr2.Name() {
		t.Errorf("name mismatch: %q vs %q", tr1.Name(), tr2.Name())
	}
}

// TestLoad_MissingFile verifies an error is returned for a non-existent source.
func TestLoad_MissingFile(t *testing.T) {
	l := newLoader(t)
	_, err := l.Load("missing", "../../testdata/transforms/does_not_exist.go", "Foo")
	if err == nil {
		t.Fatal("expected error for missing source file, got nil")
	}
}

// TestLoad_WrongEntrypoint verifies an error is returned when the function
// name doesn't exist in the compiled plugin.
func TestLoad_WrongEntrypoint(t *testing.T) {
	l := newLoader(t)
	_, err := l.Load("filter-active", filterSrc, "NonExistentFunc")
	if err == nil {
		t.Fatal("expected error for unknown entrypoint, got nil")
	}
}

// ---------------------------------------------------------------------------
// Country lookup mapping tests
// ---------------------------------------------------------------------------

// TestLoad_CountryLookup_KnownCode verifies that a recognised dial code is
// mapped to the correct country name.
func TestLoad_CountryLookup_KnownCode(t *testing.T) {
	cases := []struct {
		code    string
		country string
	}{
		{"1", "United States"},
		{"55", "Brazil"},
		{"49", "Germany"},
		{"44", "United Kingdom"},
		{"81", "Japan"},
		{"91", "India"},
		{"33", "France"},
		{"86", "China"},
	}

	l := newLoader(t)
	tr, err := l.Load("country-lookup", countryLookSrc, "CountryLookup")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	for _, tc := range cases {
		tc := tc
		t.Run("code_"+tc.code, func(t *testing.T) {
			rec := makeRecord(t, map[string]any{
				"id":           "1",
				"name":         "Alice",
				"country_code": tc.code,
			})
			out, err := tr.Apply(context.Background(), rec)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if len(out) != 1 {
				t.Fatalf("expected 1 output record, got %d", len(out))
			}

			var row map[string]any
			if err := json.Unmarshal(out[0].Value, &row); err != nil {
				t.Fatalf("unmarshal output: %v", err)
			}
			if row["country_name"] != tc.country {
				t.Errorf("code %q: expected country_name=%q, got %q",
					tc.code, tc.country, row["country_name"])
			}
		})
	}
}

// TestLoad_CountryLookup_UnknownCode verifies that an unrecognised dial code
// results in country_name = "Unknown" and the record is still passed through.
func TestLoad_CountryLookup_UnknownCode(t *testing.T) {
	l := newLoader(t)
	tr, err := l.Load("country-lookup", countryLookSrc, "CountryLookup")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	rec := makeRecord(t, map[string]any{
		"id":           "8",
		"name":         "Heidi",
		"country_code": "999", // not in the map
	})
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 output record (unknown codes pass through), got %d", len(out))
	}

	var row map[string]any
	json.Unmarshal(out[0].Value, &row)
	if row["country_name"] != "Unknown" {
		t.Errorf("expected country_name=Unknown for unrecognised code, got %q", row["country_name"])
	}
}

// TestLoad_CountryLookup_OriginalFieldsPreserved verifies that all original
// fields are still present after the mapping — the transform adds a field,
// it does not replace the whole record.
func TestLoad_CountryLookup_OriginalFieldsPreserved(t *testing.T) {
	l := newLoader(t)
	tr, err := l.Load("country-lookup", countryLookSrc, "CountryLookup")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	rec := makeRecord(t, map[string]any{
		"id":           "3",
		"name":         "Carol",
		"country_code": "49",
	})
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	var row map[string]any
	json.Unmarshal(out[0].Value, &row)

	for _, field := range []string{"id", "name", "country_code", "country_name"} {
		if _, ok := row[field]; !ok {
			t.Errorf("field %q missing from output record", field)
		}
	}
	if row["country_code"] != "49" {
		t.Errorf("original country_code was modified, got %q", row["country_code"])
	}
	if row["country_name"] != "Germany" {
		t.Errorf("expected Germany, got %q", row["country_name"])
	}
}
