package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/config"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/events"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/handler"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/service"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/store"
)

type consumerResult struct {
	name string
	err  error
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() (result error) {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := store.New(ctx, cfg.PostgresDSN)
	if err != nil {
		return err
	}
	defer db.Close()

	prod := events.NewProducer(cfg.KafkaBrokers, cfg.KafkaTopic)
	defer func() {
		if err := prod.Close(); err != nil {
			if result != nil {
				slog.Error("close kafka producer", "err", err)
				return
			}
			result = err
		}
	}()

	svc := service.New(db, prod)
	bffHandler, err := handler.NewBFFKafkaHandler(svc, prod, cfg.KafkaReplyTopics)
	if err != nil {
		return fmt.Errorf("configure BFF Kafka handler: %w", err)
	}
	bffConsumer := events.NewGroupConsumer(cfg.KafkaBrokers, cfg.KafkaBFFTopics, cfg.KafkaConsumerGroup)
	defer func() {
		if err := bffConsumer.Close(); err != nil {
			if result != nil {
				slog.Error("close BFF Kafka consumer", "err", err)
				return
			}
			result = err
		}
	}()

	routesHandler, err := handler.NewRoutesValidationKafkaHandler(svc, prod, cfg.KafkaReplyTopics)
	if err != nil {
		return fmt.Errorf("configure Routes validation Kafka handler: %w", err)
	}
	routesConsumer := events.NewGroupConsumer(cfg.KafkaBrokers, cfg.KafkaValidationTopics, cfg.KafkaRoutesGroup)
	defer func() {
		if err := routesConsumer.Close(); err != nil {
			if result != nil {
				slog.Error("close Routes validation Kafka consumer", "err", err)
				return
			}
			result = err
		}
	}()

	outboxPublisher, err := events.NewOutboxPublisher(db, prod)
	if err != nil {
		return fmt.Errorf("configure outbox publisher: %w", err)
	}

	var workers sync.WaitGroup
	defer func() {
		stop()
		workers.Wait()
	}()

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler.New(svc),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		WriteTimeout:      15 * time.Second,
	}

	slog.Info("listening", "addr", srv.Addr, "env", cfg.Env)
	serverErr := make(chan error, 1)
	consumerErr := make(chan consumerResult, 3)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()
	workers.Add(3)
	go func() {
		defer workers.Done()
		slog.Info("consuming BFF Kafka commands", "topics", cfg.KafkaBFFTopics, "group", cfg.KafkaConsumerGroup)
		err := bffConsumer.Run(ctx, bffHandler.Handle)
		if err == nil && ctx.Err() == nil {
			err = errors.New("consumer stopped unexpectedly")
		}
		consumerErr <- consumerResult{name: "BFF commands", err: err}
	}()
	go func() {
		defer workers.Done()
		slog.Info("consuming Routes bus validations", "topics", cfg.KafkaValidationTopics, "group", cfg.KafkaRoutesGroup)
		err := routesConsumer.Run(ctx, routesHandler.Handle)
		if err == nil && ctx.Err() == nil {
			err = errors.New("consumer stopped unexpectedly")
		}
		consumerErr <- consumerResult{name: "Routes validation", err: err}
	}()
	go func() {
		defer workers.Done()
		slog.Info("publishing outbox events", "topic", cfg.KafkaTopic)
		err := outboxPublisher.Run(ctx)
		if err == nil && ctx.Err() == nil {
			err = errors.New("publisher stopped unexpectedly")
		}
		consumerErr <- consumerResult{name: "Outbox publisher", err: err}
	}()

	shutdownHTTP := func() error {
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shCtx); err != nil {
			_ = srv.Close()
			return fmt.Errorf("shutdown http server: %w", err)
		}
		if err := <-serverErr; err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("serve http: %w", err)
		}
		return nil
	}

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("serve http: %w", err)
		}
	case result := <-consumerErr:
		if ctx.Err() != nil {
			if err := shutdownHTTP(); err != nil {
				return err
			}
			if result.err != nil {
				return fmt.Errorf("%s consumer stopped: %w", result.name, result.err)
			}
			for i := 0; i < 2; i++ {
				other := <-consumerErr
				if other.err != nil {
					return fmt.Errorf("%s consumer stopped: %w", other.name, other.err)
				}
			}
			return nil
		}
		return fmt.Errorf("%s consumer stopped: %w", result.name, result.err)
	case <-ctx.Done():
		if err := shutdownHTTP(); err != nil {
			return err
		}
		for i := 0; i < 3; i++ {
			result := <-consumerErr
			if result.err != nil {
				return fmt.Errorf("stop %s consumer: %w", result.name, result.err)
			}
		}
	}
	return nil
}
