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

## Demo

Watch it work against the live deployed server — a webhook delivered on the first attempt, a duplicate absorbed by its idempotency key, and a failing receiver retried with backoff, then dead-lettered after 5 attempts:

https://github.com/user-attachments/assets/b28cc44b-2138-4782-adf2-2feb3d8e1af3

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
