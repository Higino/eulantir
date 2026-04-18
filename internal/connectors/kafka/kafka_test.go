package kafka

import (
	"testing"
)

// ---------------------------------------------------------------------------
// Unit tests — configuration validation only (no live Kafka broker needed)
// ---------------------------------------------------------------------------

// TestSource_Open_MissingTopic verifies that Open returns an error when the
// required "topic" config key is absent.
func TestSource_Open_MissingTopic(t *testing.T) {
	s := &Source{}
	err := s.Open(nil, map[string]any{
		"brokers": "localhost:9092",
		// "topic" intentionally omitted
	})
	if err == nil {
		t.Fatal("expected error for missing topic, got nil")
	}
}

// TestSink_Open_MissingTopic verifies that Open returns an error when the
// required "topic" config key is absent.
func TestSink_Open_MissingTopic(t *testing.T) {
	s := &Sink{}
	err := s.Open(nil, map[string]any{
		"brokers": "localhost:9092",
		// "topic" intentionally omitted
	})
	if err == nil {
		t.Fatal("expected error for missing topic, got nil")
	}
}

// TestSource_Name verifies the connector type name.
func TestSource_Name(t *testing.T) {
	s := &Source{}
	if s.Name() != "kafka" {
		t.Errorf("expected Name()='kafka', got %q", s.Name())
	}
}

// TestSink_Name verifies the connector type name.
func TestSink_Name(t *testing.T) {
	s := &Sink{}
	if s.Name() != "kafka-sink" {
		t.Errorf("expected Name()='kafka-sink', got %q", s.Name())
	}
}

// TestSource_Close_NilReader verifies that Close is safe when Open was never called.
func TestSource_Close_NilReader(t *testing.T) {
	s := &Source{}
	if err := s.Close(nil); err != nil {
		t.Errorf("Close on uninitialised source: %v", err)
	}
}

// TestSink_Close_NilWriter verifies that Close is safe when Open was never called.
func TestSink_Close_NilWriter(t *testing.T) {
	s := &Sink{}
	if err := s.Close(nil); err != nil {
		t.Errorf("Close on uninitialised sink: %v", err)
	}
}

// TestSink_WriteBatch_EmptyBatch verifies that an empty batch is a no-op.
func TestSink_WriteBatch_EmptyBatch(t *testing.T) {
	s := &Sink{} // writer is nil — should not be called for empty batch
	if err := s.WriteBatch(nil, nil); err != nil {
		t.Errorf("WriteBatch(empty): %v", err)
	}
}

// TestStringVal_Default verifies that stringVal returns the default when key absent.
func TestStringVal_Default(t *testing.T) {
	got := stringVal(map[string]any{}, "missing", "default-value")
	if got != "default-value" {
		t.Errorf("expected 'default-value', got %q", got)
	}
}

// TestStringVal_Present verifies that stringVal returns the value when key exists.
func TestStringVal_Present(t *testing.T) {
	got := stringVal(map[string]any{"key": "hello"}, "key", "fallback")
	if got != "hello" {
		t.Errorf("expected 'hello', got %q", got)
	}
}
