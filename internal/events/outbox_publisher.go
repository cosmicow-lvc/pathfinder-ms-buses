package events

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

type OutboxStore interface {
	DeliverNextOutbox(context.Context, func(context.Context, []byte, []byte) error) (bool, error)
}

type OutboxEventPublisher interface {
	Publish(context.Context, []byte, []byte) error
}

type OutboxPublisher struct {
	store          OutboxStore
	publisher      OutboxEventPublisher
	pollInterval   time.Duration
	retryInterval  time.Duration
	maxRetry       time.Duration
	publishTimeout time.Duration
}

func NewOutboxPublisher(store OutboxStore, publisher OutboxEventPublisher) (*OutboxPublisher, error) {
	if store == nil {
		return nil, errors.New("outbox store is required")
	}
	if publisher == nil {
		return nil, errors.New("outbox event publisher is required")
	}
	return &OutboxPublisher{
		store:          store,
		publisher:      publisher,
		pollInterval:   500 * time.Millisecond,
		retryInterval:  time.Second,
		maxRetry:       30 * time.Second,
		publishTimeout: 10 * time.Second,
	}, nil
}

func (p *OutboxPublisher) Run(ctx context.Context) error {
	retryInterval := p.retryInterval
	for {
		if ctx.Err() != nil {
			return nil
		}

		delivered, err := p.store.DeliverNextOutbox(ctx, func(ctx context.Context, key, payload []byte) error {
			publishCtx, cancel := context.WithTimeout(ctx, p.publishTimeout)
			defer cancel()
			return p.publisher.Publish(publishCtx, key, payload)
		})
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			slog.Error("outbox delivery failed; retrying", "err", err, "retry_in", retryInterval)
			if !waitFor(ctx, retryInterval) {
				return nil
			}
			retryInterval *= 2
			if retryInterval > p.maxRetry {
				retryInterval = p.maxRetry
			}
			continue
		}
		if delivered {
			retryInterval = p.retryInterval
			continue
		}
		if !waitFor(ctx, p.pollInterval) {
			return nil
		}
	}
}

func waitFor(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
