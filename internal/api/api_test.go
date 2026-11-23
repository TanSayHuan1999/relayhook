package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"relayhook/internal/store"
	"relayhook/internal/testdb"
)

func testStore(t *testing.T) *store.Store {
	t.Helper()
	return testdb.Open(t)
}

func postWebhook(t *testing.T, baseURL, target, key, body string) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, baseURL+"/webhooks", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if target != "" {
		req.Header.Set("X-Relayhook-Target", target)
	}
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(New(nil).Routes())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
	}
}

func TestIdempotentPost(t *testing.T) {
	st := testStore(t)
	ctx := context.Background()
	srv := httptest.NewServer(New(st).Routes())
	defer srv.Close()

	key := fmt.Sprintf("test-%d", os.Getpid())
	code, first := postWebhook(t, srv.URL, "http://127.0.0.1:1/hook", key, `{"event":"test"}`)
	if code != http.StatusAccepted {
		t.Fatalf("first POST status = %d, want 202 (body %v)", code, first)
	}
	if first["status"] != "queued" {
		t.Fatalf("first POST status field = %v, want queued", first["status"])
	}

	code, second := postWebhook(t, srv.URL, "http://127.0.0.1:1/hook", key, `{"event":"test"}`)
	if code != http.StatusOK {
		t.Fatalf("duplicate POST status = %d, want 200", code)
	}
	if second["id"] != first["id"] {
		t.Fatalf("duplicate POST returned id %v, want existing %v", second["id"], first["id"])
	}

	// A third create with the same key still returns the existing row
	// instead of inserting a duplicate.
	d, created, err := st.CreateDelivery(ctx, key, "http://127.0.0.1:1/hook", []byte(`{"event":"test"}`))
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("third create with same key created a duplicate")
	}
	if d.ID.String() != first["id"] {
		t.Fatalf("store id %s != api id %v", d.ID, first["id"])
	}

	// GET /deliveries/{id} returns the full status shape.
	resp, err := http.Get(srv.URL + "/deliveries/" + d.ID.String())
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET delivery status = %d, want 200", resp.StatusCode)
	}
	var got map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "queued" || got["attempts"].(float64) != 0 {
		t.Fatalf("GET delivery = %v, want queued with 0 attempts", got)
	}
	if hist, ok := got["attempt_history"].([]any); !ok || len(hist) != 0 {
		t.Fatalf("attempt_history = %v, want empty list", got["attempt_history"])
	}
}

func TestPostValidation(t *testing.T) {
	st := testStore(t)
	srv := httptest.NewServer(New(st).Routes())
	defer srv.Close()

	if code, _ := postWebhook(t, srv.URL, "", "k1", `{"a":1}`); code != http.StatusBadRequest {
		t.Errorf("missing target: status = %d, want 400", code)
	}
	if code, _ := postWebhook(t, srv.URL, "ftp://example.com/x", "k2", `{"a":1}`); code != http.StatusBadRequest {
		t.Errorf("bad scheme: status = %d, want 400", code)
	}
	if code, _ := postWebhook(t, srv.URL, "http://example.com/x", "k3", `not json`); code != http.StatusBadRequest {
		t.Errorf("invalid json: status = %d, want 400", code)
	}
}

func TestGetDeliveryNotFound(t *testing.T) {
	st := testStore(t)
	srv := httptest.NewServer(New(st).Routes())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/deliveries/00000000-0000-0000-0000-000000000000")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}
