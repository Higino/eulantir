package s3

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/higino/eulantir/internal/connector"
)

// Sink writes records to a single S3 object.
//
// Supported formats (set via "format" config key):
//   - "ndjson" (default) — newline-delimited JSON; best for data lake / Lakehouse
//   - "csv"              — standard CSV with a header row
//
// Records are buffered in memory and uploaded via a single PutObject call on Close().
// For very large datasets, prefer splitting records across multiple pipeline runs.
//
// Authentication uses the standard AWS credential chain
// (env vars → ~/.aws/credentials → EC2 instance role).
// Explicit keys can be provided via "access_key_id" / "secret_access_key" config.
type Sink struct {
	bucket string
	key    string
	format string
	region string

	accessKey   string
	secretKey   string
	endpointURL string

	buf     bytes.Buffer
	csvW    *csv.Writer
	headers []string
}

func (s *Sink) Name() string { return "s3-sink" }

func (s *Sink) Open(_ context.Context, cfg map[string]any) error {
	s.bucket = stringVal(cfg, "bucket", "")
	if s.bucket == "" {
		return fmt.Errorf("s3 sink: missing required config key \"bucket\"")
	}
	s.key = stringVal(cfg, "key", "")
	if s.key == "" {
		return fmt.Errorf("s3 sink: missing required config key \"key\"")
	}
	s.format = strings.ToLower(stringVal(cfg, "format", "ndjson"))
	s.region = stringVal(cfg, "region", "us-east-1")
	s.accessKey = stringVal(cfg, "access_key_id", "")
	s.secretKey = stringVal(cfg, "secret_access_key", "")
	// endpoint_url is only needed for local S3-compatible servers (LocalStack, MinIO).
	// Leave it empty when targeting real AWS — the SDK resolves the correct endpoint automatically.
	s.endpointURL = stringVal(cfg, "endpoint_url", "")

	if s.format == "csv" {
		s.csvW = csv.NewWriter(&s.buf)
	}
	return nil
}

// WriteBatch buffers records. The actual upload happens in Close().
func (s *Sink) WriteBatch(_ context.Context, records []connector.Record) error {
	switch s.format {
	case "csv":
		return s.writeCSV(records)
	default:
		return s.writeNDJSON(records)
	}
}

func (s *Sink) writeNDJSON(records []connector.Record) error {
	for _, r := range records {
		if _, err := s.buf.Write(r.Value); err != nil {
			return fmt.Errorf("s3 sink: buffer ndjson: %w", err)
		}
		if err := s.buf.WriteByte('\n'); err != nil {
			return fmt.Errorf("s3 sink: buffer newline: %w", err)
		}
	}
	return nil
}

func (s *Sink) writeCSV(records []connector.Record) error {
	for _, r := range records {
		var row map[string]any
		if err := json.Unmarshal(r.Value, &row); err != nil {
			return fmt.Errorf("s3 sink: unmarshal record: %w", err)
		}
		if s.headers == nil {
			for k := range row {
				s.headers = append(s.headers, k)
			}
			sort.Strings(s.headers)
			if err := s.csvW.Write(s.headers); err != nil {
				return fmt.Errorf("s3 sink: write csv header: %w", err)
			}
		}
		vals := make([]string, len(s.headers))
		for i, h := range s.headers {
			vals[i] = fmt.Sprintf("%v", row[h])
		}
		if err := s.csvW.Write(vals); err != nil {
			return fmt.Errorf("s3 sink: write csv row: %w", err)
		}
	}
	s.csvW.Flush()
	return s.csvW.Error()
}

// Close flushes the buffer and uploads the object to S3.
func (s *Sink) Close(ctx context.Context) error {
	if s.buf.Len() == 0 {
		return nil
	}

	var opts []func(*awsconfig.LoadOptions) error
	opts = append(opts, awsconfig.WithRegion(s.region))
	if s.accessKey != "" && s.secretKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(s.accessKey, s.secretKey, ""),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return fmt.Errorf("s3 sink: load AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if s.endpointURL != "" {
		ep := s.endpointURL
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = &ep
			o.UsePathStyle = true // LocalStack/MinIO require path-style URLs; AWS virtual-hosted-style is the default
		})
	}

	client := s3.NewFromConfig(awsCfg, s3Opts...)
	body := bytes.NewReader(s.buf.Bytes())
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    &s.key,
		Body:   body,
	})
	if err != nil {
		return fmt.Errorf("s3 sink: put object s3://%s/%s: %w", s.bucket, s.key, err)
	}
	return nil
}

func init() {
	connector.Default.RegisterSink(connector.ConnectorInfo{
		Type:        "s3-sink",
		Description: "Write records to an S3 object (NDJSON or CSV)",
		ConfigKeys:  []string{"bucket", "key", "region", "format", "access_key_id", "secret_access_key", "endpoint_url"},
	}, func(ctx context.Context, cfg map[string]any) (connector.Sink, error) {
		sk := &Sink{}
		if err := sk.Open(ctx, cfg); err != nil {
			return nil, err
		}
		return sk, nil
	})
}
