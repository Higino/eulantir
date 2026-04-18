package dlq

import (
	"context"
	"errors"
	"testing"

	"github.com/higino/eulantir/internal/connector"
)

func TestFileDLQ_PushAndCount(t *testing.T) {
	dir := t.TempDir()
	d := NewFileDLQ(dir)
	ctx := context.Background()

	rec := connector.Record{Value: []byte(`{"id":"1"}`), Offset: 1}
	if err := d.Push(ctx, "task-a", rec, errors.New("write failed")); err != nil {
		t.Fatalf("Push failed: %v", err)
	}
	if err := d.Push(ctx, "task-a", rec, errors.New("write failed")); err != nil {
		t.Fatalf("Push failed: %v", err)
	}

	count, err := d.Count(ctx, "task-a")
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("expected 2, got %d", count)
	}
}

func TestFileDLQ_Drain(t *testing.T) {
	dir := t.TempDir()
	d := NewFileDLQ(dir)
	ctx := context.Background()

	for i := range 3 {
		rec := connector.Record{Value: []byte(`{}`), Offset: int64(i)}
		d.Push(ctx, "node-x", rec, errors.New("err"))
	}

	ch, err := d.Drain(ctx, "node-x")
	if err != nil {
		t.Fatal(err)
	}

	var got int
	for range ch {
		got++
	}
	if got != 3 {
		t.Errorf("expected 3 drained records, got %d", got)
	}
}

func TestFileDLQ_EmptyCount(t *testing.T) {
	d := NewFileDLQ(t.TempDir())
	count, _ := d.Count(context.Background(), "nobody")
	if count != 0 {
		t.Errorf("expected 0, got %d", count)
	}
}
