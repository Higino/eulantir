package profiler

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/higino/eulantir/internal/connector"
)

// FieldType classifies the dominant data type observed in a field.
type FieldType string

const (
	TypeString  FieldType = "string"
	TypeNumber  FieldType = "number"
	TypeBoolean FieldType = "boolean"
	TypeObject  FieldType = "object"
	TypeArray   FieldType = "array"
	TypeMixed   FieldType = "mixed"
)

// FieldProfile is the statistical summary of one field across sampled records.
type FieldProfile struct {
	Name        string    `json:"name"`
	Type        FieldType `json:"type"`
	NullRate    float64   `json:"null_rate"`
	Cardinality int64     `json:"cardinality"`
	Examples    []string  `json:"examples,omitempty"`
	Pattern     string    `json:"pattern,omitempty"`
	Min         *float64  `json:"min,omitempty"`
	Max         *float64  `json:"max,omitempty"`
	Mean        *float64  `json:"mean,omitempty"`
	MinLen      *int      `json:"min_len,omitempty"`
	MaxLen      *int      `json:"max_len,omitempty"`
}

// DatasetProfile is the complete statistical profile of one source connector.
type DatasetProfile struct {
	Source    string         `json:"source"`
	SampledAt time.Time      `json:"sampled_at"`
	TotalSeen int64          `json:"total_seen"`
	Fields    []FieldProfile `json:"fields"`
}

// Sample reads up to sampleSize records from src and returns them.
// It consumes records in batches until the source is exhausted or the limit is reached.
func Sample(ctx context.Context, src connector.Source, sampleSize int) ([]connector.Record, error) {
	var records []connector.Record
	for len(records) < sampleSize {
		remaining := sampleSize - len(records)
		if remaining > 256 {
			remaining = 256
		}
		batch, err := src.ReadBatch(ctx, remaining)
		records = append(records, batch...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
	}
	return records, nil
}

// Profile computes a DatasetProfile from a slice of records.
// sourceName is the connector name or type, used for display.
func Profile(sourceName string, records []connector.Record) (*DatasetProfile, error) {
	accums := map[string]*fieldAccum{}

	for _, rec := range records {
		var row map[string]any
		if err := json.Unmarshal(rec.Value, &row); err != nil {
			continue
		}
		for k, v := range row {
			a, ok := accums[k]
			if !ok {
				a = &fieldAccum{name: k, seen: map[string]struct{}{}}
				accums[k] = a
			}
			a.observe(v)
		}
	}

	// Sort field names for deterministic output.
	names := make([]string, 0, len(accums))
	for n := range accums {
		names = append(names, n)
	}
	sort.Strings(names)

	fields := make([]FieldProfile, 0, len(names))
	for _, n := range names {
		fields = append(fields, accums[n].summarize(int64(len(records))))
	}

	return &DatasetProfile{
		Source:    sourceName,
		SampledAt: time.Now().UTC(),
		TotalSeen: int64(len(records)),
		Fields:    fields,
	}, nil
}

// ── pattern detection ──────────────────────────────────────────────────────

var (
	reEmail     = regexp.MustCompile(`(?i)^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$`)
	reUUID      = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	reDate      = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	reTimestamp = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}`)
	rePhone     = regexp.MustCompile(`^\+?[\d\s\-().]{7,20}$`)
	reURL       = regexp.MustCompile(`(?i)^https?://`)
)

type patternDef struct {
	name string
	re   *regexp.Regexp
}

var patterns = []patternDef{
	{"email", reEmail},
	{"uuid", reUUID},
	{"timestamp", reTimestamp},
	{"date", reDate},
	{"url", reURL},
	{"phone", rePhone},
}

// ── field accumulator ──────────────────────────────────────────────────────

const numPatterns = 6 // must equal len(patterns)

type fieldAccum struct {
	name     string
	nulls    int64
	total    int64
	seen     map[string]struct{}
	examples []string

	typeStrings  int64
	typeNumbers  int64
	typeBooleans int64
	typeObjects  int64
	typeArrays   int64

	patternHits [numPatterns]int64

	numSum float64
	numMin float64
	numMax float64
	numN   int64

	strMinLen int
	strMaxLen int
	strFirst  bool
}

func (a *fieldAccum) observe(v any) {
	a.total++

	if v == nil {
		a.nulls++
		return
	}

	switch val := v.(type) {
	case bool:
		a.typeBooleans++

	case float64:
		a.typeNumbers++
		if a.numN == 0 {
			a.numMin = val
			a.numMax = val
		} else {
			if val < a.numMin {
				a.numMin = val
			}
			if val > a.numMax {
				a.numMax = val
			}
		}
		a.numSum += val
		a.numN++

	case string:
		a.typeStrings++
		l := len(val)
		if a.strFirst {
			if l < a.strMinLen {
				a.strMinLen = l
			}
			if l > a.strMaxLen {
				a.strMaxLen = l
			}
		} else {
			a.strMinLen = l
			a.strMaxLen = l
			a.strFirst = true
		}
		// check string patterns
		for i, p := range patterns {
			if p.re.MatchString(val) {
				a.patternHits[i]++
			}
		}
		// collect examples (up to 5 unique)
		if _, dup := a.seen[val]; !dup && len(a.examples) < 5 {
			a.seen[val] = struct{}{}
			a.examples = append(a.examples, val)
		}
		if _, dup := a.seen[val]; !dup {
			a.seen[val] = struct{}{}
		}

	case map[string]any:
		a.typeObjects++

	case []any:
		a.typeArrays++

	default:
		// try string coercion (e.g. json.Number)
		if s, ok := v.(interface{ String() string }); ok {
			a.observe(s.String())
			return
		}
		// try numeric coercion
		if s := strconv.FormatFloat(toFloat(v), 'f', -1, 64); s != "<nil>" {
			a.typeNumbers++
		}
	}
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	}
	return math.NaN()
}

func (a *fieldAccum) summarize(sampleTotal int64) FieldProfile {
	fp := FieldProfile{
		Name:        a.name,
		NullRate:    float64(a.nulls) / float64(a.total),
		Cardinality: int64(len(a.seen)),
		Examples:    a.examples,
	}

	// dominant type
	dominant := a.dominant()
	fp.Type = dominant

	// numeric stats
	if dominant == TypeNumber && a.numN > 0 {
		mean := a.numSum / float64(a.numN)
		fp.Min = &a.numMin
		fp.Max = &a.numMax
		fp.Mean = &mean
	}

	// string stats + pattern detection
	if dominant == TypeString && a.typeStrings > 0 {
		fp.MinLen = &a.strMinLen
		fp.MaxLen = &a.strMaxLen

		// a pattern must match ≥80% of non-null string values
		threshold := int64(math.Ceil(float64(a.typeStrings) * 0.8))
		for i, p := range patterns {
			if a.patternHits[i] >= threshold {
				fp.Pattern = p.name
				break
			}
		}
	}

	return fp
}

func (a *fieldAccum) dominant() FieldType {
	counts := []struct {
		t FieldType
		n int64
	}{
		{TypeString, a.typeStrings},
		{TypeNumber, a.typeNumbers},
		{TypeBoolean, a.typeBooleans},
		{TypeObject, a.typeObjects},
		{TypeArray, a.typeArrays},
	}

	nonNullTotal := a.total - a.nulls
	if nonNullTotal == 0 {
		return TypeString
	}

	var best FieldType
	var bestN int64
	distinctTypes := 0
	for _, c := range counts {
		if c.n > 0 {
			distinctTypes++
			if c.n > bestN {
				bestN = c.n
				best = c.t
			}
		}
	}

	// more than one type and no single type owns ≥80% → mixed
	if distinctTypes > 1 && float64(bestN)/float64(nonNullTotal) < 0.8 {
		return TypeMixed
	}
	if best == "" {
		return TypeString
	}
	return best
}
