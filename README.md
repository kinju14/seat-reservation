# Seat Reservation at Scale

Sells assigned seats and never double-sells, over-books a user, or double-charges a retry.
FastAPI + asyncpg + Postgres. Containerised. Prometheus metrics, JSON logs, one-command burst.

**Live URL:** `https://seat-reservation-met6.onrender.com`  **Admin token:** attached in email

## Run locally
```bash
docker compose up --build          # API on :8000, Postgres behind it
ulimit -n 10240
ADMIN_TOKEN=dev-admin-token ./burst.sh http://localhost:8000
```

## One-command burst (against the live URL)
```bash
ADMIN_TOKEN=<token from submission email> ./burst.sh https://seat-reservation-met6.onrender.com        # or: make burst URL=... ADMIN_TOKEN=...
# knobs: --total 20000 --users 5000 --seats 2000 --hot 20 --hot-users 500 --concurrency 2000
```
Prints the outcome distribution per phase (confirmed / declined-by-reason / 5xx), then asserts: exactly one
201 per hot seat, no seat in two reservations, `available+held+confirmed == total` **polled during the burst**
and after, zero 5xx, per-user limit, idempotency, spoofed identity, and that `/metrics` deltas equal what the
client observed. Exit code is non-zero if any check fails. *Wake a free-tier instance first:* `curl https://seat-reservation-met6.onrender.com/readyz`.

## API
| | |
|---|---|
| `POST /auth/token` `{"user_id":"alice"}` | dev token minting (open on purpose so you can create users; `ENABLE_DEV_AUTH=false` disables) |
| `POST /shows` (admin: `Authorization: Bearer $ADMIN_TOKEN`) | `{"name","seats":[...],"price_paise":25000,"per_user_limit":4}` |
| `POST /shows/{id}/reserve` (user JWT) | `{"seats":["A12"],"idempotency_key":"..."}` or `Idempotency-Key` header |
| `POST /reservations/{id}/cancel` (owner only) | frees the seats; idempotent |
| `GET /shows/{id}[?include_seats=false]` | per-seat status + `counts` + `invariant_ok` |
| `GET /healthz` · `GET /readyz` · `GET /metrics` | liveness · readiness (DB check, fails closed → 503) · Prometheus |

**Endpoints:** [/healthz](https://seat-reservation-met6.onrender.com/healthz) · [/readyz](https://seat-reservation-met6.onrender.com/readyz) · [/metrics](https://seat-reservation-met6.onrender.com/metrics) · [/docs](https://seat-reservation-met6.onrender.com/docs)

**Behaviour contract**
- Multi-seat requests are **all-or-nothing**. Nothing is reserved unless every seat is free.
- Declines are 4xx: `409 seat_taken`, `409 per_user_limit`, `409 idempotency_key_conflict`, `404 unknown_seat`.
- Replays return the **original** outcome (201 + same body, or the same decline) with header `Idempotent-Replay: true`.
- Model: reserving immediately **confirms**; release is explicit `cancel` (no TTL), so `held` is always 0.
- Identity comes only from the JWT; body fields like `user_id` are ignored. Money is integer paise (floats rejected).
- `429 overloaded` is only returned if a DB connection can't be had within `DB_ACQUIRE_TIMEOUT` (90s). Normally requests queue.

## Observability
- **Metrics** `GET /metrics`: `reservations_confirmed_total`, `reservations_declined_total{reason=seat_taken|per_user_limit|idempotent_replay|key_conflict|unknown_seat}`, `reservations_cancelled_total`,
  `seats_available{show_id}` / `seats_held` / `seats_confirmed` (computed from the DB **at scrape time**, so they reconcile with `GET /shows/{id}`; newest 20 shows),
  `db_up`, `db_write_pool_in_use`, `http_requests_total{route,method,status}`, `http_request_duration_seconds`, `http_requests_in_flight`.
- **Logs**: one JSON line per request with `request_id` (honours inbound `X-Request-ID`, echoed in the response header), `route`, `status`, `duration_ms`, `user_id`, `outcome`.
```json
{"ts": "2026-10-04T18:31:22.669Z", "level": "INFO", "logger": "app", "msg": "request", "request_id": "6618fe7c4688496bbdd7874e92716f32", "method": "POST", "path": "/shows/85afc89a-f01b-49ee-839f-cd9742ed32e3/reserve", "route": "/shows/{show_id}/reserve", "status": 409, "duration_ms": 32703.6, "user_id": "54725f-u4967", "show_id": "85afc89a-f01b-49ee-839f-cd9742ed32e3", "n_seats": 2, "outcome": "seat_taken"}
```
Public logs: https://drive.google.com/file/d/1-9-swwfA2RTJkMhgADfxhmcZoAaz-Btw/view?usp=sharing.
