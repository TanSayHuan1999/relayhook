// Package worker is the delivery loop: it claims due deliveries from
// Postgres and POSTs them to their targets, recording every attempt
// and scheduling retries with exponential backoff until the delivery
// succeeds or exhausts its attempts and lands in 'dead'.
package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"

	"relayhook/internal/deliver"
	"relayhook/internal/store"
)

// Worker polls the queue and performs deliveries.
type Worker struct {
	Store        *store.Store
	Client       *http.Client
	Secret       string
	MaxAttempts  int
	BackoffBase  time.Duration
	PollInterval time.Duration
	BatchSize    int
}

// New builds a Worker with the given policy knobs.
func New(st *store.Store, secret string, maxAttempts int, backoffBase, pollInterval, clientTimeout time.Duration) *Worker {
	return &Worker{
		Store:        st,
		Client:       &http.Client{Timeout: clientTimeout},
		Secret:       secret,
		MaxAttempts:  maxAttempts,
		BackoffBase:  backoffBase,
		PollInterval: pollInterval,
		BatchSize:    10,
	}
}

// Run polls until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.RunOnce(ctx); err != nil && ctx.Err() == nil {
				log.Printf("worker: %v", err)
			}
		}
	}
}

// RunOnce claims and processes one batch of due deliveries.
func (w *Worker) RunOnce(ctx context.Context) error {
	claimed, err := w.Store.ClaimDue(ctx, w.BatchSize)
	if err != nil {
		return err
	}
	for _, d := range claimed {
		if err := w.deliverOne(ctx, d); err != nil {
			log.Printf("worker: delivery %s: %v", d.ID, err)
		}
	}
	return nil
}

func (w *Worker) deliverOne(ctx context.Context, d *store.Delivery) error {
	attemptNo := d.Attempts + 1

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.TargetURL, bytes.NewReader(d.Payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Relayhook-Id", d.ID.String())
	req.Header.Set("X-Relayhook-Attempt", strconv.Itoa(attemptNo))
	req.Header.Set("X-Relayhook-Signature", deliver.Sign(w.Secret, d.Payload))

	start := time.Now()
	resp, err := w.Client.Do(req)
	duration := time.Since(start)

	attempt := store.Attempt{AttemptNo: attemptNo, DurationMs: int(duration.Milliseconds())}
	var failErr string
	if err != nil {
		failErr = truncate(err.Error(), 500)
		attempt.Error = &failErr
	} else {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		status := resp.StatusCode
		attempt.ResponseStatus = &status
		if status >= 200 && status < 300 {
			return w.Store.CompleteAttempt(ctx, d.ID, attempt, store.StatusDelivered, nil, nil)
		}
		failErr = fmt.Sprintf("unexpected status %d", status)
		attempt.Error = &failErr
	}

	if attemptNo >= w.MaxAttempts {
		return w.Store.CompleteAttempt(ctx, d.ID, attempt, store.StatusDead, nil, &failErr)
	}
	next := time.Now().Add(deliver.Backoff(attemptNo, w.BackoffBase) + w.jitter())
	return w.Store.CompleteAttempt(ctx, d.ID, attempt, store.StatusRetrying, &next, &failErr)
}

// jitter adds a small random delay of up to 25% of the backoff base so
// many failing deliveries do not retry in lockstep.
func (w *Worker) jitter() time.Duration {
	if w.BackoffBase <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(w.BackoffBase)/4 + 1))
}

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max]
	}
	return s
}
