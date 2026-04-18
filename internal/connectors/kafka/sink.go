package kafka

import (
	"context"
	"fmt"
	"strings"

	"github.com/higino/eulantir/internal/connector"
	kafkago "github.com/segmentio/kafka-go"
)

// Sink writes records to a Kafka topic.
// Each connector.Record is written as one Kafka message:
//   - Key   = record.Key  (may be nil)
//   - Value = record.Value (the JSON payload)
type Sink struct {
	writer *kafkago.Writer
}

func (s *Sink) Name() string { return "kafka-sink" }

func (s *Sink) Open(_ context.Context, cfg map[string]any) error {
	brokersRaw := stringVal(cfg, "brokers", "localhost:9092")
	brokers := strings.Split(brokersRaw, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	topic := stringVal(cfg, "topic", "")
	if topic == "" {
		return fmt.Errorf("kafka sink: missing required config key \"topic\"")
	}

	s.writer = &kafkago.Writer{
		Addr:                   kafkago.TCP(brokers...),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
		// Async would be faster but we want at-least-once guarantees.
		Async: false,
	}
	return nil
}

func (s *Sink) Close(_ context.Context) error {
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}

// WriteBatch publishes all records in a single batch to Kafka.
func (s *Sink) WriteBatch(ctx context.Context, batch []connector.Record) error {
	if len(batch) == 0 {
		return nil
	}

	msgs := make([]kafkago.Message, len(batch))
	for i, rec := range batch {
		msgs[i] = kafkago.Message{
			Key:   rec.Key,
			Value: rec.Value,
		}
	}

	if err := s.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("kafka sink: write messages: %w", err)
	}
	return nil
}

func init() {
	connector.Default.RegisterSink(connector.ConnectorInfo{
		Type:        "kafka-sink",
		Description: "Write records to a Kafka topic",
		ConfigKeys:  []string{"brokers", "topic"},
	}, func(ctx context.Context, cfg map[string]any) (connector.Sink, error) {
		s := &Sink{}
		if err := s.Open(ctx, cfg); err != nil {
			return nil, err
		}
		return s, nil
	})
}

