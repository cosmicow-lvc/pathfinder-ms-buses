package main

import (
	"context"
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

	srv := &http.Server{
		Addr:              ":" + cfg.Port,
		Handler:           handler.New(service.New(db, prod)),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		WriteTimeout:      15 * time.Second,
	}

	slog.Info("listening", "addr", srv.Addr, "env", cfg.Env)
	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("serve http: %w", err)
		}
	case <-ctx.Done():
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shCtx); err != nil {
			_ = srv.Close()
			return fmt.Errorf("shutdown http server: %w", err)
		}
		if err := <-serverErr; err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("serve http: %w", err)
		}
	}
	return nil
}
