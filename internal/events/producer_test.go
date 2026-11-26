package events

import (
	"context"
	"errors"
	"testing"

	"github.com/segmentio/kafka-go"
)

type kafkaWriterStub struct {
	messages []kafka.Message
	writeErr error
	closeErr error
}

func (w *kafkaWriterStub) WriteMessages(_ context.Context, messages ...kafka.Message) error {
	w.messages = append(w.messages, messages...)
	return w.writeErr
}

func (w *kafkaWriterStub) Close() error {
	return w.closeErr
}

func TestProducerPublishesToConfiguredAndRequestedTopics(t *testing.T) {
	writer := &kafkaWriterStub{}
	producer := newProducer(writer, "events.default")

	if err := producer.Publish(context.Background(), []byte("key-1"), []byte("value-1")); err != nil {
		t.Fatal(err)
	}
	if err := producer.PublishTo(context.Background(), "events.reply", []byte("key-2"), []byte("value-2")); err != nil {
		t.Fatal(err)
	}
	if len(writer.messages) != 2 {
		t.Fatalf("expected two messages, got %d", len(writer.messages))
	}
	if writer.messages[0].Topic != "events.default" || string(writer.messages[0].Key) != "key-1" {
		t.Fatalf("unexpected default-topic message: %+v", writer.messages[0])
	}
	if writer.messages[1].Topic != "events.reply" || string(writer.messages[1].Value) != "value-2" {
		t.Fatalf("unexpected explicit-topic message: %+v", writer.messages[1])
	}
}

func TestProducerReportsPublishAndCloseErrors(t *testing.T) {
	writeErr := errors.New("write failed")
	closeErr := errors.New("close failed")
	writer := &kafkaWriterStub{writeErr: writeErr, closeErr: closeErr}
	producer := newProducer(writer, "events.default")

	if err := producer.Publish(context.Background(), nil, nil); !errors.Is(err, writeErr) {
		t.Fatalf("expected publish error, got %v", err)
	}
	if err := producer.PublishTo(context.Background(), "", nil, nil); err == nil {
		t.Fatal("expected an error for an empty topic")
	}
	if err := producer.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected close error, got %v", err)
	}
}
