package kafka

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/higino/eulantir/internal/connector"
	kafkago "github.com/segmentio/kafka-go"
)

// Source reads records from a Kafka topic using a consumer group.
// Each Kafka message becomes one connector.Record:
//   - Key   = message key bytes
//   - Value = message value bytes (assumed to be JSON)
//   - Offset = partition offset
type Source struct {
	reader *kafkago.Reader
}

func (s *Source) Name() string { return "kafka" }

func (s *Source) Open(_ context.Context, cfg map[string]any) error {
	brokersRaw := stringVal(cfg, "brokers", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	topic := stringVal(cfg, "topic", "")
	if topic == "" {
		return fmt.Errorf("kafka source: missing required config key \"topic\"")
	}

	groupID := stringVal(cfg, "group_id", "eulantir")

	startOffset := kafkago.LastOffset
	if stringVal(cfg, "offset", "latest") == "earliest" {
		startOffset = kafkago.FirstOffset
	}

	s.reader = kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		StartOffset: startOffset,
		MinBytes:    1,
		MaxBytes:    10 << 20, // 10 MiB
		MaxWait:     1 * time.Second,
	})
	return nil
}

func (s *Source) Close(_ context.Context) error {
	if s.reader != nil {
		return s.reader.Close()
	}
	return nil
}

// ReadBatch fetches up to maxSize messages from Kafka.
// Returns io.EOF when the context is cancelled (signals pipeline shutdown).
func (s *Source) ReadBatch(ctx context.Context, maxSize int) ([]connector.Record, error) {
	var records []connector.Record

	for len(records) < maxSize {
		// FetchMessage does NOT commit the offset — Commit() does.
		msg, err := s.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || err == io.EOF {
				if len(records) > 0 {
					return records, nil
				}
				return nil, io.EOF
			}
			return records, fmt.Errorf("kafka source: fetch message: %w", err)
		}

		records = append(records, connector.Record{
			Key:    msg.Key,
			Value:  msg.Value,
			Offset: msg.Offset,
		})
	}
	return records, nil
}

// Commit marks messages up to and including offset as processed.
func (s *Source) Commit(ctx context.Context, offset int64) error {
	msg := kafkago.Message{Offset: offset}
	return s.reader.CommitMessages(ctx, msg)
}

func init() {
	connector.Default.RegisterSource(connector.ConnectorInfo{
		Type:        "kafka",
		Description: "Read records from a Kafka topic (consumer group)",
		ConfigKeys:  []string{"brokers", "topic", "group_id", "offset"},
	}, func(ctx context.Context, cfg map[string]any) (connector.Source, error) {
		s := &Source{}
		if err := s.Open(ctx, cfg); err != nil {
			return nil, err
		}
		return s, nil
	})
}
