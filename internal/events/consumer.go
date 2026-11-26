package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader kafkaReader
}

type kafkaReader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
	Close() error
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return newConsumer(kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		CommitInterval: 0,
	}))
}

func NewGroupConsumer(brokers, topics []string, groupID string) *Consumer {
	return newConsumer(kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupTopics:    topics,
		GroupID:        groupID,
		CommitInterval: 0,
	}))
}

func newConsumer(reader kafkaReader) *Consumer {
	return &Consumer{reader: reader}
}

func (c *Consumer) Run(ctx context.Context, handle func(context.Context, kafka.Message) error) error {
	for {
		message, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("fetch kafka message: %w", err)
		}
		if err := handle(ctx, message); err != nil {
			return fmt.Errorf("handle kafka message at offset %d: %w", message.Offset, err)
		}
		if err := c.reader.CommitMessages(ctx, message); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("commit kafka message at offset %d: %w", message.Offset, err)
		}
	}
}

func (c *Consumer) Close() error {
	if err := c.reader.Close(); err != nil {
		return fmt.Errorf("close kafka consumer: %w", err)
	}
	return nil
}
