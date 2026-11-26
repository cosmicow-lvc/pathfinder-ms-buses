package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func (s *Store) DeliverNextOutbox(
	ctx context.Context,
	deliver func(context.Context, []byte, []byte) error,
) (bool, error) {
	if deliver == nil {
		return false, errors.New("outbox delivery function is required")
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin outbox delivery transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(context.Background())
	}()

	var id int64
	var key string
	var payload []byte
	err = tx.QueryRow(ctx, `
		SELECT id, COALESCE(payload->'data'->>'busId', ''), payload
		FROM outbox
		WHERE publicado = FALSE
		ORDER BY id
		LIMIT 1
		FOR UPDATE
	`).Scan(&id, &key, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get pending outbox message: %w", err)
	}

	if err := deliver(ctx, []byte(key), payload); err != nil {
		return true, fmt.Errorf("deliver outbox message %d: %w", id, err)
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET publicado = TRUE WHERE id = $1`, id); err != nil {
		return true, fmt.Errorf("mark outbox message %d as published: %w", id, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("commit outbox message %d delivery: %w", id, err)
	}
	return true, nil
}
