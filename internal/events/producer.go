package events

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer kafkaWriter
	topic  string
}

type kafkaWriter interface {
	WriteMessages(context.Context, ...kafka.Message) error
	Close() error
}

func NewProducer(brokers []string, topic string) *Producer {
	return newProducer(&kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Balancer: &kafka.LeastBytes{},
	}, topic)
}

func newProducer(writer kafkaWriter, topic string) *Producer {
	return &Producer{writer: writer, topic: topic}
}

func (p *Producer) Publish(ctx context.Context, key, value []byte) error {
	return p.PublishTo(ctx, p.topic, key, value)
}

func (p *Producer) PublishTo(ctx context.Context, topic string, key, value []byte) error {
	if topic == "" {
		return fmt.Errorf("publish kafka message: topic is required")
	}
	if err := p.writer.WriteMessages(ctx, kafka.Message{Topic: topic, Key: key, Value: value}); err != nil {
		return fmt.Errorf("publish kafka message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close kafka producer: %w", err)
	}
	return nil
}
