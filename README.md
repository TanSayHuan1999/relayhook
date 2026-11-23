# relayhook

A small webhook delivery service in Go + PostgreSQL. It accepts webhooks
over HTTP and delivers them to subscriber endpoints **reliably**: retries
with exponential backoff, idempotency keys so an event is never delivered
twice, HMAC-signed payloads, and a dead-letter state for events that
exhaust their attempts.

PostgreSQL is the job queue — no Redis, no message broker. Workers claim
due deliveries with `SELECT ... FOR UPDATE SKIP LOCKED`, which is the
right amount of infrastructure at this scale and the interesting design
decision in the project.

## How it works

```
sender ──POST /webhooks──▶ API ──insert──▶ PostgreSQL (deliveries queue)
                                                │
                          worker polls due rows with SKIP LOCKED
                                                │
                                                ▼
receiver ◀──POST payload + HMAC signature── worker
   │                                            │
   └── 2xx ──▶ delivered                        ├── failure ──▶ retrying
                                                │    (backoff 2s, 4s, 8s…)
                                                └── attempts exhausted ──▶ dead
```

## API

### Send a webhook

```bash
curl -X POST localhost:8080/webhooks \
  -H "X-Relayhook-Target: https://example.com/hooks/orders" \
  -H "Idempotency-Key: order-42-paid" \
  -H "Content-Type: application/json" \
  -d '{"order": 42, "event": "paid"}'
# 202 {"id": "…", "status": "queued", "idempotency_key": "order-42-paid"}
```

Send the same `Idempotency-Key` again and you get the **existing**
delivery back (200) — nothing is queued or delivered twice. The key is
optional; one is generated if you leave it out.

### Check a delivery

```bash
curl localhost:8080/deliveries/<id>
```

Returns the delivery's status (`queued`, `delivering`, `retrying`,
`delivered`, `dead`) and every recorded attempt: response status, error,
duration, timestamp.

### What receivers get

Each delivery attempt is a POST of the original payload with:

- `X-Relayhook-Id` — the delivery id
- `X-Relayhook-Attempt` — the attempt number
- `X-Relayhook-Signature` — `sha256=<hex HMAC-SHA256 of the payload>`,
  keyed with the server's signing secret, so receivers can verify the
  webhook really came from relayhook

Any 2xx response counts as delivered. Anything else is retried with
exponential backoff plus jitter, up to the max attempts, then the
delivery is marked `dead` and kept for inspection — never silently
dropped.

## Design decisions worth talking about

- **Postgres as the queue.** `FOR UPDATE SKIP LOCKED` lets multiple
  workers claim rows without stepping on each other, and the delivery
  state lives in the same transaction as the attempt record. A broker
  would add a second system to run before it adds any value.
- **Idempotency at insert.** A unique constraint on the idempotency key
  plus `ON CONFLICT DO NOTHING` makes duplicate submissions a database
  guarantee, not application hope.
- **Stuck-delivery recovery.** Rows left in `delivering` by a crashed
  worker are reclaimed after 30 seconds, so a crash mid-delivery delays
  an event instead of losing it.
- **One transaction per attempt outcome.** The attempt row and the
  delivery's state transition commit together — the history can never
  disagree with the status.

One honest nuance: payloads are stored as `jsonb`, and Postgres
re-serialises jsonb (key order and spacing can change). The worker signs
the stored bytes — exactly the bytes it sends — so receiver-side
verification still holds. Byte-exact passthrough would store a `bytea`
copy alongside; it is noted as future work rather than hidden.

## Run it

With Docker:

```bash
cp .env.example .env
docker compose up
```

Locally (needs Go and a Postgres to point at):

```bash
DATABASE_URL=postgres://relayhook:relayhook@localhost:5432/relayhook go run ./cmd/relayhook
```

The schema is created automatically at startup. Configuration is
environment-only: `DATABASE_URL` (required), `PORT` (8080),
`RELAYHOOK_SIGNING_SECRET`, `RELAYHOOK_MAX_ATTEMPTS` (5),
`RELAYHOOK_BACKOFF_BASE_SECONDS` (2).

## Tests

```bash
DATABASE_URL=postgres://relayhook:relayhook@localhost:5432/relayhook go test ./...
```

Unit tests cover the backoff schedule and HMAC signing (known-answer
vector). Integration tests run the real worker against a throwaway
database per test package: a receiver that fails twice then succeeds
ends `delivered` with 3 recorded attempts; an always-failing receiver
ends `dead`; a duplicate idempotency key produces exactly one delivery.

Verified end to end against a live server: retry-then-succeed delivered
on attempt 3 with backoff-spaced attempt timestamps, duplicate
submission returned the same delivery id with no extra receiver request,
and an always-500 receiver exhausted its attempts into the dead state.

## License

MIT
