package worker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"relayhook/internal/deliver"
	"relayhook/internal/store"
	"relayhook/internal/testdb"
)

const testSecret = "test-signing-secret"

func testStore(t *testing.T) *store.Store {
	t.Helper()
	return testdb.Open(t)
}

func testWorker(st *store.Store, maxAttempts int) *Worker {
	return New(st, testSecret, maxAttempts, 10*time.Millisecond, 50*time.Millisecond, 2*time.Second)
}

// drive runs worker cycles, waiting out the tiny backoff between them,
// until the delivery reaches a terminal state or cycles run out.
func drive(t *testing.T, ctx context.Context, w *Worker, st *store.Store, id uuid.UUID, cycles int) *store.Delivery {
	t.Helper()
	for i := 0; i < cycles; i++ {
		if err := w.RunOnce(ctx); err != nil {
			t.Fatalf("RunOnce: %v", err)
		}
		d, err := st.GetDelivery(ctx, id)
		if err != nil {
			t.Fatalf("GetDelivery: %v", err)
		}
		if d.Status == store.StatusDelivered || d.Status == store.StatusDead {
			return d
		}
		time.Sleep(60 * time.Millisecond)
	}
	d, _ := st.GetDelivery(ctx, id)
	return d
}

func TestWorkerDeliversAfterRetries(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	var hits atomic.Int32
	var gotSig atomic.Value
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		gotSig.Store(r.Header.Get("X-Relayhook-Signature"))
		if hits.Add(1) <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer receiver.Close()

	payload := []byte(`{"order":42}`)
	d, created, err := st.CreateDelivery(ctx, fmt.Sprintf("wk-ok-%d", os.Getpid()), receiver.URL, payload)
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}

	w := testWorker(st, 5)
	final := drive(t, ctx, w, st, d.ID, 12)
	if final.Status != store.StatusDelivered {
		t.Fatalf("status = %s, want delivered (last_error=%v)", final.Status, final.LastError)
	}
	if final.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3", final.Attempts)
	}
	if hits.Load() != 3 {
		t.Fatalf("receiver hits = %d, want 3", hits.Load())
	}
	// Postgres jsonb re-serialises the payload (e.g. adds spaces), so
	// the worker signs the stored bytes — which are exactly the bytes
	// it sends, so a receiver verifying the HMAC over the received
	// body succeeds. Compare against the stored payload, not the raw
	// input.
	if sig := gotSig.Load(); sig != deliver.Sign(testSecret, d.Payload) {
		t.Fatalf("signature header = %v, want %s (HMAC of stored payload %q)", sig, deliver.Sign(testSecret, d.Payload), d.Payload)
	}

	history, err := st.ListAttempts(ctx, d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 3 {
		t.Fatalf("attempt rows = %d, want 3", len(history))
	}
	wantStatuses := []int{500, 500, 200}
	for i, a := range history {
		if a.ResponseStatus == nil || *a.ResponseStatus != wantStatuses[i] {
			t.Errorf("attempt %d response_status = %v, want %d", a.AttemptNo, a.ResponseStatus, wantStatuses[i])
		}
	}
}

func TestWorkerDeadAfterMaxAttempts(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()

	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer receiver.Close()

	d, _, err := st.CreateDelivery(ctx, fmt.Sprintf("wk-dead-%d", os.Getpid()), receiver.URL, []byte(`{"x":1}`))
	if err != nil {
		t.Fatal(err)
	}

	w := testWorker(st, 2)
	final := drive(t, ctx, w, st, d.ID, 10)
	if final.Status != store.StatusDead {
		t.Fatalf("status = %s, want dead", final.Status)
	}
	if final.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2", final.Attempts)
	}
	if final.NextAttemptAt != nil {
		t.Fatalf("dead delivery has next_attempt_at %v, want nil", final.NextAttemptAt)
	}
}
