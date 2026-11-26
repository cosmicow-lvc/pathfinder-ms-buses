package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/config"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/events"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/handler"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/service"
	"github.com/cosmicow-lvc/pathfinder-ms-buses/internal/store"
)

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
	bffConsumer := events.NewConsumer(cfg.KafkaBrokers, cfg.KafkaCommandsTopic, cfg.KafkaConsumerGroup)
	defer func() {
		if err := bffConsumer.Close(); err != nil {
			if result != nil {
				slog.Error("close BFF Kafka consumer", "err", err)
				return
			}
			result = err
		}
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
	consumerErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()
	go func() {
		slog.Info("consuming BFF Kafka commands", "topic", cfg.KafkaCommandsTopic, "group", cfg.KafkaConsumerGroup)
		consumerErr <- bffConsumer.Run(ctx, bffHandler.Handle)
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
	case err := <-consumerErr:
		if ctx.Err() != nil {
			if err := shutdownHTTP(); err != nil {
				return err
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("consume BFF Kafka commands: %w", err)
		}
		return errors.New("BFF Kafka consumer stopped unexpectedly")
	case <-ctx.Done():
		if err := shutdownHTTP(); err != nil {
			return err
		}
		if err := <-consumerErr; err != nil {
			return fmt.Errorf("stop BFF Kafka consumer: %w", err)
		}
	}
	return nil
}
