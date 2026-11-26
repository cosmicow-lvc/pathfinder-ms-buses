package events

import (
	"context"
	"errors"
	"testing"
	"time"
)

type outboxStoreStub struct {
	calls  int
	cancel context.CancelFunc
}

func (s *outboxStoreStub) DeliverNextOutbox(
	ctx context.Context,
	deliver func(context.Context, []byte, []byte) error,
) (bool, error) {
	s.calls++
	if s.calls == 1 {
		return false, errors.New("temporary database error")
	}
	if err := deliver(ctx, []byte("bus-123"), []byte(`{"type":"bus.status.changed.v1"}`)); err != nil {
		return true, err
	}
	s.cancel()
	return true, nil
}

type outboxEventPublisherStub struct {
	calls   int
	key     []byte
	payload []byte
}

func (p *outboxEventPublisherStub) Publish(_ context.Context, key, payload []byte) error {
	p.calls++
	p.key = append([]byte(nil), key...)
	p.payload = append([]byte(nil), payload...)
	return nil
}

func TestOutboxPublisherRetriesAndPublishesPendingEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	store := &outboxStoreStub{cancel: cancel}
	producer := &outboxEventPublisherStub{}
	publisher, err := NewOutboxPublisher(store, producer)
	if err != nil {
		t.Fatal(err)
	}
	publisher.retryInterval = time.Millisecond

	if err := publisher.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if store.calls != 2 {
		t.Fatalf("expected one retry and a successful delivery, got %d attempts", store.calls)
	}
	if producer.calls != 1 || string(producer.key) != "bus-123" {
		t.Fatalf("unexpected Kafka publish calls/key: calls=%d key=%q", producer.calls, producer.key)
	}
	if string(producer.payload) != `{"type":"bus.status.changed.v1"}` {
		t.Fatalf("unexpected published payload: %s", producer.payload)
	}
}
