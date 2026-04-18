package s3

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/higino/eulantir/internal/connector"
)

// Source reads records from a single S3 object.
//
// Supported formats (set via "format" config key):
//   - "csv"   (default) — standard CSV with a header row; each row → JSON record
//   - "ndjson"          — newline-delimited JSON; each line → one record
//
// Authentication uses the standard AWS credential chain
// (env vars → ~/.aws/credentials → EC2 instance role).
// Explicit keys can be provided via "access_key_id" / "secret_access_key" config.
type Source struct {
	bucket string
	key    string
	format string

	body    io.ReadCloser
	offset  int64

	// CSV mode
	csvReader *csv.Reader
	headers   []string

	// NDJSON mode
	scanner *bufio.Scanner
}

func (s *Source) Name() string { return "s3" }

func (s *Source) Open(ctx context.Context, cfg map[string]any) error {
	s.bucket = stringVal(cfg, "bucket", "")
	if s.bucket == "" {
		return fmt.Errorf("s3 source: missing required config key \"bucket\"")
	}
	s.key = stringVal(cfg, "key", "")
	if s.key == "" {
		return fmt.Errorf("s3 source: missing required config key \"key\"")
	}
	s.format = strings.ToLower(stringVal(cfg, "format", "csv"))

	region := stringVal(cfg, "region", "us-east-1")

	// Build AWS config — explicit keys override the default chain.
	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(region))

	accessKey := stringVal(cfg, "access_key_id", "")
	secretKey := stringVal(cfg, "secret_access_key", "")
	if accessKey != "" && secretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("s3 source: load AWS config: %w", err)
	}

	// endpoint_url is only needed for local S3-compatible servers (LocalStack, MinIO).
	// Leave it empty when targeting real AWS — the SDK resolves the correct endpoint automatically.
	endpointURL := stringVal(cfg, "endpoint_url", "")
	var s3Opts []func(*s3.Options)
	if endpointURL != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &endpointURL
			o.UsePathStyle = true // LocalStack/MinIO require path-style URLs; AWS virtual-hosted-style is the default
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
	})
	if err != nil {
		return fmt.Errorf("s3 source: get object s3://%s/%s: %w", s.bucket, s.key, err)
	}
	s.body = out.Body

	switch s.format {
	case "ndjson":
		s.scanner = bufio.NewScanner(s.body)
		s.scanner.Buffer(make([]byte, 1<<20), 10<<20) // 10 MiB max line
	default: // csv
		s.csvReader = csv.NewReader(s.body)
		s.csvReader.TrimLeadingSpace = true
		hdrs, err := s.csvReader.Read()
		if err != nil {
			return fmt.Errorf("s3 source: read CSV headers: %w", err)
		}
		s.headers = hdrs
	}
	return nil
}

func (s *Source) Close(_ context.Context) error {
	if s.body != nil {
		return s.body.Close()
	}
	return nil
}

// ReadBatch reads up to maxSize records from the S3 object.
// Returns io.EOF when the object is fully consumed.
func (s *Source) ReadBatch(_ context.Context, maxSize int) ([]connector.Record, error) {
	switch s.format {
	case "ndjson":
		return s.readNDJSON(maxSize)
	default:
		return s.readCSV(maxSize)
	}
}

func (s *Source) readCSV(maxSize int) ([]connector.Record, error) {
	var records []connector.Record
	for len(records) < maxSize {
		row, err := s.csvReader.Read()
		if err == io.EOF {
			if len(records) == 0 {
				return nil, io.EOF
			}
			return records, nil
		}
		if err != nil {
			return records, fmt.Errorf("s3 source: read CSV row: %w", err)
		}

		rowMap := make(map[string]string, len(s.headers))
		for i, h := range s.headers {
			if i < len(row) {
				rowMap[h] = row[i]
			}
		}
		val, err := json.Marshal(rowMap)
		if err != nil {
			return records, fmt.Errorf("s3 source: marshal row: %w", err)
		}
		s.offset++
		records = append(records, connector.Record{Value: val, Offset: s.offset})
	}
	return records, nil
}

func (s *Source) readNDJSON(maxSize int) ([]connector.Record, error) {
	var records []connector.Record
	for len(records) < maxSize {
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return records, fmt.Errorf("s3 source: scan ndjson: %w", err)
			}
			if len(records) == 0 {
				return nil, io.EOF
			}
			return records, nil
		}
		line := strings.TrimSpace(s.scanner.Text())
		if line == "" {
			continue
		}
		s.offset++
		records = append(records, connector.Record{Value: []byte(line), Offset: s.offset})
	}
	return records, nil
}

func (s *Source) Commit(_ context.Context, _ int64) error {
	// S3 objects are immutable; no offset tracking needed.
	return nil
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
	connector.Default.RegisterSource(connector.ConnectorInfo{
		Type:        "s3",
		Description: "Read records from an S3 object (CSV or NDJSON)",
		ConfigKeys:  []string{"bucket", "key", "region", "format", "access_key_id", "secret_access_key", "endpoint_url"},
	}, func(ctx context.Context, cfg map[string]any) (connector.Source, error) {
		src := &Source{}
		if err := src.Open(ctx, cfg); err != nil {
			return nil, err
		}
		return src, nil
	})
}
