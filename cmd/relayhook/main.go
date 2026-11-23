// relayhook accepts webhooks over HTTP and delivers them reliably:
// retries with exponential backoff, idempotency keys, HMAC signing,
// and a dead-letter state — with Postgres as the job queue.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"relayhook/internal/api"
	"relayhook/internal/config"
	"relayhook/internal/store"
	"relayhook/internal/worker"
)

func main() {
	if err := run(); err != nil {
		log.Fatalf("relayhook: %v", err)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		return err
	}
	log.Printf("relayhook: schema ready, max_attempts=%d backoff_base=%s",
		cfg.MaxAttempts, cfg.BackoffBase)

	w := worker.New(st, cfg.SigningSecret, cfg.MaxAttempts, cfg.BackoffBase,
		cfg.PollInterval, cfg.DeliverTimeout)
	go w.Run(ctx)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           api.New(st).Routes(),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("relayhook: listening on :%d", cfg.Port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}
