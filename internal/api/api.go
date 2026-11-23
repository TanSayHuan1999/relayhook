// Package api exposes the HTTP surface: webhook intake with
// idempotency, delivery status lookup, and a health check.
package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"relayhook/internal/store"
)

// API holds the handlers' dependencies.
type API struct {
	Store *store.Store
}

// New returns an API bound to the store.
func New(st *store.Store) *API { return &API{Store: st} }

// Routes returns the mux with all endpoints registered.
func (a *API) Routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", a.healthz)
	mux.HandleFunc("POST /webhooks", a.createWebhook)
	mux.HandleFunc("GET /deliveries/{id}", a.getDelivery)
	return mux
}

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

type createResponse struct {
	ID             uuid.UUID `json:"id"`
	Status         string    `json:"status"`
	IdempotencyKey string    `json:"idempotency_key"`
}

func (a *API) createWebhook(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Relayhook-Target")
	if target == "" {
		writeError(w, http.StatusBadRequest, "missing X-Relayhook-Target header")
		return
	}
	u, err := url.Parse(target)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		writeError(w, http.StatusBadRequest, "X-Relayhook-Target must be an http(s) URL")
		return
	}

	payload, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeError(w, http.StatusBadRequest, "could not read body")
		return
	}
	if len(payload) == 0 || !json.Valid(payload) {
		writeError(w, http.StatusBadRequest, "body must be a JSON payload")
		return
	}

	key := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if key == "" {
		key = generateKey()
	}

	d, created, err := a.Store.CreateDelivery(r.Context(), key, target, payload)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not store delivery")
		return
	}
	status := http.StatusAccepted
	if !created {
		// Same idempotency key: hand back the existing delivery,
		// no duplicate is created or sent.
		status = http.StatusOK
	}
	writeJSON(w, status, createResponse{ID: d.ID, Status: d.Status, IdempotencyKey: d.IdempotencyKey})
}

type deliveryResponse struct {
	*store.Delivery
	AttemptHistory []store.Attempt `json:"attempt_history"`
}

func (a *API) getDelivery(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "id must be a UUID")
		return
	}
	d, err := a.Store.GetDelivery(r.Context(), id)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "delivery not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load delivery")
		return
	}
	history, err := a.Store.ListAttempts(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load attempts")
		return
	}
	writeJSON(w, http.StatusOK, deliveryResponse{Delivery: d, AttemptHistory: history})
}

func generateKey() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return uuid.NewString()
	}
	return "gen_" + hex.EncodeToString(b)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
