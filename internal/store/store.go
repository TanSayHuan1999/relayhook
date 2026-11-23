// Package store is the Postgres persistence layer. Postgres doubles as
// the job queue: the worker claims due deliveries with
// SELECT ... FOR UPDATE SKIP LOCKED, so multiple workers can run
// without a separate message broker.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Delivery statuses.
const (
	StatusQueued     = "queued"
	StatusRetrying   = "retrying"
	StatusDelivering = "delivering"
	StatusDelivered  = "delivered"
	StatusDead       = "dead"
)

// Delivery is one webhook to be delivered (eventually) to TargetURL.
type Delivery struct {
	ID             uuid.UUID  `json:"id"`
	IdempotencyKey string     `json:"idempotency_key"`
	TargetURL      string     `json:"target_url"`
	Payload        []byte     `json:"-"`
	Status         string     `json:"status"`
	Attempts       int        `json:"attempts"`
	NextAttemptAt  *time.Time `json:"next_attempt_at"`
	LastError      *string    `json:"last_error"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// Attempt is a single recorded delivery try.
type Attempt struct {
	AttemptNo      int       `json:"attempt_no"`
	ResponseStatus *int      `json:"response_status"`
	Error          *string   `json:"error"`
	DurationMs     int       `json:"duration_ms"`
	CreatedAt      time.Time `json:"created_at"`
}

// Store wraps the connection pool.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a Store from a Postgres connection string.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() { s.pool.Close() }

const schema = `
CREATE TABLE IF NOT EXISTS deliveries (
	id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
	idempotency_key text UNIQUE NOT NULL,
	target_url      text NOT NULL,
	payload         jsonb NOT NULL,
	status          text NOT NULL DEFAULT 'queued',
	attempts        int NOT NULL DEFAULT 0,
	next_attempt_at timestamptz,
	last_error      text,
	created_at      timestamptz NOT NULL DEFAULT now(),
	updated_at      timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS attempts (
	id              bigserial PRIMARY KEY,
	delivery_id     uuid NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
	attempt_no      int NOT NULL,
	response_status int,
	error           text,
	duration_ms     int NOT NULL,
	created_at      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_deliveries_due
	ON deliveries (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_attempts_delivery
	ON attempts (delivery_id, attempt_no);
`

// migrateLockKey serialises concurrent migrations (e.g. several
// instances booting at once) so CREATE TABLE IF NOT EXISTS cannot
// race itself.
const migrateLockKey = 727272

// Migrate creates the schema if it does not exist yet.
func (s *Store) Migrate(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("migrate: acquire conn: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrateLockKey); err != nil {
		return fmt.Errorf("migrate: lock: %w", err)
	}
	defer func() { _, _ = conn.Exec(ctx, `SELECT pg_advisory_unlock($1)`, migrateLockKey) }()
	if _, err := conn.Exec(ctx, schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	return nil
}

const deliveryColumns = `id, idempotency_key, target_url, payload, status,
	attempts, next_attempt_at, last_error, created_at, updated_at`

func scanDelivery(row pgx.Row) (*Delivery, error) {
	var d Delivery
	err := row.Scan(&d.ID, &d.IdempotencyKey, &d.TargetURL, &d.Payload,
		&d.Status, &d.Attempts, &d.NextAttemptAt, &d.LastError,
		&d.CreatedAt, &d.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &d, nil
}

// CreateDelivery inserts a new queued delivery. If a delivery with the
// same idempotency key already exists, it returns that delivery with
// created=false and inserts nothing — the duplicate is never delivered
// twice.
func (s *Store) CreateDelivery(ctx context.Context, idempotencyKey, targetURL string, payload []byte) (d *Delivery, created bool, err error) {
	d, err = scanDelivery(s.pool.QueryRow(ctx, `
		INSERT INTO deliveries (idempotency_key, target_url, payload, status, next_attempt_at)
		VALUES ($1, $2, $3, 'queued', now())
		ON CONFLICT (idempotency_key) DO NOTHING
		RETURNING `+deliveryColumns,
		idempotencyKey, targetURL, string(payload)))
	if err == nil {
		return d, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("insert delivery: %w", err)
	}
	// Lost the race / key already existed: fetch the existing row.
	d, err = scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE idempotency_key = $1`,
		idempotencyKey))
	if err != nil {
		return nil, false, fmt.Errorf("fetch existing delivery: %w", err)
	}
	return d, false, nil
}

// GetDelivery returns one delivery by id, or pgx.ErrNoRows.
func (s *Store) GetDelivery(ctx context.Context, id uuid.UUID) (*Delivery, error) {
	d, err := scanDelivery(s.pool.QueryRow(ctx,
		`SELECT `+deliveryColumns+` FROM deliveries WHERE id = $1`, id))
	if err != nil {
		return nil, err
	}
	return d, nil
}

// ListAttempts returns all recorded attempts for a delivery, in order.
func (s *Store) ListAttempts(ctx context.Context, deliveryID uuid.UUID) ([]Attempt, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT attempt_no, response_status, error, duration_ms, created_at
		FROM attempts WHERE delivery_id = $1 ORDER BY attempt_no ASC`, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("list attempts: %w", err)
	}
	defer rows.Close()
	out := []Attempt{}
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.AttemptNo, &a.ResponseStatus, &a.Error, &a.DurationMs, &a.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan attempt: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ClaimDue atomically claims up to limit due deliveries: rows in
// 'queued'/'retrying' whose next_attempt_at has passed, plus
// 'delivering' rows stale for over 30s (a worker died mid-delivery).
// Claimed rows move to 'delivering' so no other worker picks them up.
func (s *Store) ClaimDue(ctx context.Context, limit int) ([]*Delivery, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin claim tx: %w", err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT `+deliveryColumns+` FROM deliveries
		WHERE (status IN ('queued', 'retrying') AND next_attempt_at <= now())
		   OR (status = 'delivering' AND updated_at < now() - interval '30 seconds')
		ORDER BY next_attempt_at ASC NULLS FIRST
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, fmt.Errorf("select due deliveries: %w", err)
	}
	var claimed []*Delivery
	var ids []uuid.UUID
	for rows.Next() {
		d, err := scanDelivery(rows)
		if err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan claimed delivery: %w", err)
		}
		claimed = append(claimed, d)
		ids = append(ids, d.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed deliveries: %w", err)
	}
	if len(ids) == 0 {
		return nil, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE deliveries SET status = 'delivering', updated_at = now()
		WHERE id = ANY($1)`, ids); err != nil {
		return nil, fmt.Errorf("mark delivering: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim tx: %w", err)
	}
	for _, d := range claimed {
		d.Status = StatusDelivering
	}
	return claimed, nil
}

// CompleteAttempt records one delivery attempt and moves the delivery
// to its next state (delivered / retrying with a scheduled time / dead)
// in a single transaction.
func (s *Store) CompleteAttempt(ctx context.Context, deliveryID uuid.UUID, attempt Attempt, newStatus string, nextAttemptAt *time.Time, lastError *string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin complete tx: %w", err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `
		INSERT INTO attempts (delivery_id, attempt_no, response_status, error, duration_ms)
		VALUES ($1, $2, $3, $4, $5)`,
		deliveryID, attempt.AttemptNo, attempt.ResponseStatus, attempt.Error, attempt.DurationMs); err != nil {
		return fmt.Errorf("insert attempt: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE deliveries
		SET attempts = $2, status = $3, next_attempt_at = $4,
		    last_error = $5, updated_at = now()
		WHERE id = $1`,
		deliveryID, attempt.AttemptNo, newStatus, nextAttemptAt, lastError); err != nil {
		return fmt.Errorf("update delivery: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit complete tx: %w", err)
	}
	return nil
}
