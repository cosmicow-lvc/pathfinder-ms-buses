package events

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/segmentio/kafka-go"
)

type kafkaReaderStub struct {
	messages   []kafka.Message
	fetchErr   error
	commitErr  error
	closeErr   error
	committed  []kafka.Message
	fetchCalls int
}

func (r *kafkaReaderStub) FetchMessage(context.Context) (kafka.Message, error) {
	r.fetchCalls++
	if len(r.messages) > 0 {
		message := r.messages[0]
		r.messages = r.messages[1:]
		return message, nil
	}
	if r.fetchErr != nil {
		return kafka.Message{}, r.fetchErr
	}
	return kafka.Message{}, context.Canceled
}

func (r *kafkaReaderStub) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	r.committed = append(r.committed, messages...)
	return r.commitErr
}

func (r *kafkaReaderStub) Close() error {
	return r.closeErr
}

func TestConsumerCommitsOnlyAfterHandlerSucceeds(t *testing.T) {
	message := kafka.Message{Offset: 12, Value: []byte("payload")}
	reader := &kafkaReaderStub{messages: []kafka.Message{message}}
	consumer := newConsumer(reader)
	calls := 0

	if err := consumer.Run(context.Background(), func(_ context.Context, got kafka.Message) error {
		calls++
		if got.Offset != message.Offset {
			t.Fatalf("expected offset %d, got %d", message.Offset, got.Offset)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || len(reader.committed) != 1 || reader.committed[0].Offset != message.Offset {
		t.Fatalf("unexpected handler/commit counts: calls=%d committed=%+v", calls, reader.committed)
	}
}

func TestConsumerDoesNotCommitWhenHandlerFails(t *testing.T) {
	reader := &kafkaReaderStub{messages: []kafka.Message{{Offset: 7}}}
	consumer := newConsumer(reader)
	handlerErr := errors.New("handler failed")

	err := consumer.Run(context.Background(), func(context.Context, kafka.Message) error {
		return handlerErr
	})
	if !errors.Is(err, handlerErr) || !strings.Contains(err.Error(), "offset 7") {
		t.Fatalf("expected handler error with offset, got %v", err)
	}
	if len(reader.committed) != 0 {
		t.Fatalf("failed message must not be committed: %+v", reader.committed)
	}
}

func TestConsumerReportsFetchAndCommitErrors(t *testing.T) {
	t.Run("fetch", func(t *testing.T) {
		fetchErr := errors.New("broker unavailable")
		consumer := newConsumer(&kafkaReaderStub{fetchErr: fetchErr})
		if err := consumer.Run(context.Background(), func(context.Context, kafka.Message) error { return nil }); !errors.Is(err, fetchErr) {
			t.Fatalf("expected fetch error, got %v", err)
		}
	})

	t.Run("commit", func(t *testing.T) {
		commitErr := errors.New("commit failed")
		reader := &kafkaReaderStub{messages: []kafka.Message{{Offset: 9}}, commitErr: commitErr}
		consumer := newConsumer(reader)
		err := consumer.Run(context.Background(), func(context.Context, kafka.Message) error { return nil })
		if !errors.Is(err, commitErr) || !strings.Contains(err.Error(), "offset 9") {
			t.Fatalf("expected commit error with offset, got %v", err)
		}
	})
}

func TestConsumerCloseReportsReaderError(t *testing.T) {
	closeErr := errors.New("reader close failed")
	consumer := newConsumer(&kafkaReaderStub{closeErr: closeErr})

	if err := consumer.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("expected reader close error, got %v", err)
	}
}
