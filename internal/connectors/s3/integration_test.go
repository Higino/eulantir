package s3_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/higino/eulantir/internal/config"
	"github.com/higino/eulantir/internal/connector"
	"github.com/higino/eulantir/internal/engine"

	// register s3 source + sink via init()
	_ "github.com/higino/eulantir/internal/connectors/s3"
)

// ---------------------------------------------------------------------------
// Skip guard + shared helpers
// ---------------------------------------------------------------------------

// testEndpoint returns TEST_S3_ENDPOINT or skips the test.
// Start LocalStack and export TEST_S3_ENDPOINT=http://localhost:4566 to run.
func testEndpoint(t *testing.T) string {
	t.Helper()
	ep := os.Getenv("TEST_S3_ENDPOINT")
	if ep == "" {
		t.Skip("skipping S3 integration test: TEST_S3_ENDPOINT not set " +
			"(start LocalStack and set TEST_S3_ENDPOINT=http://localhost:4566)")
	}
	return ep
}

// adminClient builds an S3 admin client aimed at LocalStack.
// LocalStack accepts any non-empty key/secret pair.
func adminClient(t *testing.T, ep string) *awss3.Client {
	t.Helper()
	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	if err != nil {
		t.Fatalf("build AWS config: %v", err)
	}
	return awss3.NewFromConfig(awsCfg, func(o *awss3.Options) {
		o.BaseEndpoint = &ep
		o.UsePathStyle = true
	})
}

// ensureBucket creates a bucket, ignoring already-exists errors.
func ensureBucket(t *testing.T, client *awss3.Client, bucket string) {
	t.Helper()
	_, err := client.CreateBucket(context.Background(), &awss3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil &&
		!strings.Contains(err.Error(), "BucketAlreadyOwnedByYou") &&
		!strings.Contains(err.Error(), "BucketAlreadyExists") {
		t.Fatalf("create bucket %q: %v", bucket, err)
	}
}

// uploadObject puts raw bytes at s3://bucket/key.
func uploadObject(t *testing.T, client *awss3.Client, bucket, key string, body []byte) {
	t.Helper()
	_, err := client.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(body),
	})
	if err != nil {
		t.Fatalf("upload s3://%s/%s: %v", bucket, key, err)
	}
}

// downloadObject retrieves bytes from s3://bucket/key.
func downloadObject(t *testing.T, client *awss3.Client, bucket, key string) []byte {
	t.Helper()
	out, err := client.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		t.Fatalf("download s3://%s/%s: %v", bucket, key, err)
	}
	defer out.Body.Close()
	b, err := io.ReadAll(out.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return b
}

// connCfg builds a connector config map for LocalStack.
func connCfg(bucket, key, format, ep string) map[string]any {
	return map[string]any{
		"bucket":            bucket,
		"key":               key,
		"format":            format,
		"region":            "us-east-1",
		"access_key_id":     "test",
		"secret_access_key": "test",
		"endpoint_url":      ep,
	}
}

// drainSource reads all records from a Source until io.EOF.
func drainSource(t *testing.T, src connector.Source) []connector.Record {
	t.Helper()
	ctx := context.Background()
	var all []connector.Record
	for {
		batch, err := src.ReadBatch(ctx, 100)
		all = append(all, batch...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("ReadBatch: %v", err)
		}
	}
	return all
}

// ---------------------------------------------------------------------------
// Test: S3 (NDJSON) → S3 (NDJSON)
// ---------------------------------------------------------------------------

func TestIntegration_S3ToS3_NDJSON(t *testing.T) {
	ep := testEndpoint(t)
	client := adminClient(t, ep)
	ensureBucket(t, client, "eulantir-test")

	inputNDJSON := `{"id":"1","name":"Alice"}` + "\n" +
		`{"id":"2","name":"Bob"}` + "\n" +
		`{"id":"3","name":"Carol"}` + "\n"
	uploadObject(t, client, "eulantir-test", "input.ndjson", []byte(inputNDJSON))

	ctx := context.Background()

	// --- source ---
	src, err := connector.Default.BuildSource("s3", connCfg("eulantir-test", "input.ndjson", "ndjson", ep))
	if err != nil {
		t.Fatalf("build source: %v", err)
	}
	defer src.Close(ctx)

	records := drainSource(t, src)
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// --- sink ---
	snk, err := connector.Default.BuildSink("s3-sink", connCfg("eulantir-test", "output.ndjson", "ndjson", ep))
	if err != nil {
		t.Fatalf("build sink: %v", err)
	}
	if err := snk.WriteBatch(ctx, records); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := snk.Close(ctx); err != nil {
		t.Fatalf("sink Close (upload): %v", err)
	}

	// --- verify output ---
	out := downloadObject(t, client, "eulantir-test", "output.ndjson")
	scanner := bufio.NewScanner(bytes.NewReader(out))
	var names []string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var row map[string]any
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatalf("parse line: %v", err)
		}
		names = append(names, row["name"].(string))
	}
	if len(names) != 3 {
		t.Fatalf("expected 3 output lines, got %d", len(names))
	}
	if names[0] != "Alice" || names[1] != "Bob" || names[2] != "Carol" {
		t.Errorf("unexpected names: %v", names)
	}
}

// ---------------------------------------------------------------------------
// Test: S3 (CSV) → S3 (CSV)
// ---------------------------------------------------------------------------

func TestIntegration_S3ToS3_CSV(t *testing.T) {
	ep := testEndpoint(t)
	client := adminClient(t, ep)
	ensureBucket(t, client, "eulantir-test")

	inputCSV := "id,city\n1,NYC\n2,LON\n3,TYO\n"
	uploadObject(t, client, "eulantir-test", "cities.csv", []byte(inputCSV))

	ctx := context.Background()

	src, err := connector.Default.BuildSource("s3", connCfg("eulantir-test", "cities.csv", "csv", ep))
	if err != nil {
		t.Fatalf("build source: %v", err)
	}
	defer src.Close(ctx)

	records := drainSource(t, src)
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	snk, err := connector.Default.BuildSink("s3-sink", connCfg("eulantir-test", "cities_out.csv", "csv", ep))
	if err != nil {
		t.Fatalf("build sink: %v", err)
	}
	if err := snk.WriteBatch(ctx, records); err != nil {
		t.Fatalf("WriteBatch: %v", err)
	}
	if err := snk.Close(ctx); err != nil {
		t.Fatalf("sink Close (upload): %v", err)
	}

	// verify output — expect header row + 3 data rows, headers sorted alphabetically
	out := downloadObject(t, client, "eulantir-test", "cities_out.csv")
	r := csv.NewReader(bytes.NewReader(out))
	rows, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse output CSV: %v", err)
	}
	if len(rows) != 4 { // header + 3 data
		t.Fatalf("expected 4 CSV rows (header+3), got %d", len(rows))
	}
	if rows[0][0] != "city" || rows[0][1] != "id" {
		t.Errorf("expected alphabetically sorted headers [city id], got %v", rows[0])
	}
}

// ---------------------------------------------------------------------------
// Test: full LocalEngine pipeline  S3 → S3  (NDJSON)
// ---------------------------------------------------------------------------

func TestIntegration_Pipeline_S3ToS3_NDJSON(t *testing.T) {
	ep := testEndpoint(t)
	client := adminClient(t, ep)
	ensureBucket(t, client, "eulantir-pipeline-test")

	input := `{"product":"widget","qty":"10"}` + "\n" +
		`{"product":"gadget","qty":"5"}` + "\n"
	uploadObject(t, client, "eulantir-pipeline-test", "orders.ndjson", []byte(input))

	pipelineCfg := config.PipelineConfig{
		Version: "1",
		Name:    "s3-to-s3-pipeline",
		Connectors: []config.ConnectorConfig{
			{
				Name: "src",
				Type: "s3",
				Config: map[string]any{
					"bucket": "eulantir-pipeline-test", "key": "orders.ndjson",
					"format": "ndjson", "region": "us-east-1",
					"access_key_id": "test", "secret_access_key": "test",
					"endpoint_url": ep,
				},
			},
			{
				Name: "dst",
				Type: "s3-sink",
				Config: map[string]any{
					"bucket": "eulantir-pipeline-test", "key": "orders_out.ndjson",
					"format": "ndjson", "region": "us-east-1",
					"access_key_id": "test", "secret_access_key": "test",
					"endpoint_url": ep,
				},
			},
		},
		Tasks: []config.TaskConfig{
			{ID: "read", Connector: "src", DependsOn: []string{}},
			{ID: "write", Connector: "dst", DependsOn: []string{"read"}},
		},
	}

	// Build a registry from the Default (which has s3 + s3-sink registered via init())
	reg := connector.NewRegistry()
	for _, info := range connector.Default.List() {
		localInfo := info
		reg.RegisterSource(localInfo, func(ctx context.Context, cfg map[string]any) (connector.Source, error) {
			return connector.Default.BuildSource(localInfo.Type, cfg)
		})
		reg.RegisterSink(localInfo, func(ctx context.Context, cfg map[string]any) (connector.Sink, error) {
			return connector.Default.BuildSink(localInfo.Type, cfg)
		})
	}

	eng := &engine.LocalEngine{Registry: reg}
	results, err := eng.Run(context.Background(), pipelineCfg)
	if err != nil {
		t.Fatalf("pipeline Run: %v", err)
	}

	var readRes, writeRes engine.TaskResult
	for r := range results {
		switch r.NodeID {
		case "read":
			readRes = r
		case "write":
			writeRes = r
		}
	}

	if readRes.Status != engine.StatusSuccess {
		t.Errorf("read: expected success, got %s (err: %v)", readRes.Status, readRes.Err)
	}
	if readRes.RecordsIn != 2 {
		t.Errorf("read: expected RecordsIn=2, got %d", readRes.RecordsIn)
	}
	if writeRes.Status != engine.StatusSuccess {
		t.Errorf("write: expected success, got %s (err: %v)", writeRes.Status, writeRes.Err)
	}
	if writeRes.RecordsOut != 2 {
		t.Errorf("write: expected RecordsOut=2, got %d", writeRes.RecordsOut)
	}

	// verify the object was actually written to LocalStack
	out := downloadObject(t, client, "eulantir-pipeline-test", "orders_out.ndjson")
	lines := strings.Split(strings.TrimRight(string(out), "\n"), "\n")
	if len(lines) != 2 {
		t.Errorf("expected 2 output lines, got %d: %q", len(lines), string(out))
	}
}
