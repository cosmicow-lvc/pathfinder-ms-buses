package service

import (
	"context"
	"errors"
)

type Database interface {
	Ping(context.Context) error
}

type EventPublisher interface {
	Publish(context.Context, []byte, []byte) error
}

type Service struct {
	db        Database
	publisher EventPublisher
}

func New(db Database, publisher EventPublisher) *Service {
	return &Service{db: db, publisher: publisher}
}

func (s *Service) CheckReady(ctx context.Context) error {
	return s.db.Ping(ctx)
}

func (s *Service) Publish(ctx context.Context, key, value []byte) error {
	if s.publisher == nil {
		return errors.New("event publisher is not configured")
	}
	return s.publisher.Publish(ctx, key, value)
}
