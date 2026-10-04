# WRITEUP

## 1. The atomic decision
Everything happens in **one short Postgres transaction** (`db._reserve_tx`, READ COMMITTED):

1. claim the idempotency key (unique index), 2. take a per-(show,user) advisory lock and count the user's seats,
3. `SELECT … FROM seats WHERE label = ANY(..) ORDER BY label FOR UPDATE`, check every seat is `available`, then `UPDATE`.

**Why it is race-free:** the decision ("is it available?") and the write happen while holding the seat's *row lock*.
500 buyers of A12 queue on that lock; the first commits `confirmed`; each later one re-reads the row *after* acquiring the
lock and sees `confirmed` → 409. There is no read-then-write gap, because the read is `FOR UPDATE`.
**Multi-seat / deadlock:** Reserve acquires locks in a fixed order: idempotency-key row → per-(show,user) advisory lock → seat rows sorted by label (SQL ORDER BY). Cancel locks its reservation row, then its seats in the same sorted order. Because every path takes seat locks in the same order, no wait cycle can form. No cycle can form. Deadlock/serialization errors are still
caught and retried (bounded) as a safety net. 

**Partial requests are all-or-nothing**: any unavailable seat declines the whole request
and nothing is written.

**Per-user limit:** a count is only safe if nothing else for that user can interleave, so the advisory lock
`hashtextextended(show:user)` serialises that user's reserves on that show *before* the count. 10 parallel requests from one
user run one at a time; the 5th sees 4 held. Different users never contend on it.

## 2. Idempotency
Key lives on the `reservations` row, `UNIQUE (user_id, idempotency_key)`, plus a `request_hash` of (show, sorted seats).
`INSERT … ON CONFLICT DO NOTHING` is the first statement: concurrent same-key requests block on the unique index until the first commits, then
see the conflict and return the stored outcome with `Idempotent-Replay: true`. Same key, different hash → `409 idempotency_key_conflict`.
Declines are committed against the key too, so a retried decline replays rather than re-deciding. Exactly-once is
enforced by the database, not by application code.

## 3. Holds & expiry
Chosen model: **explicit cancel**, no TTL. Reserve confirms immediately. `cancel` locks the reservation row, locks its seats in order, and
updates `WHERE reservation_id = <this one>`; so it can only free seats that reservation still owns and can never resurrect a seat now owned by
someone else. Cancelling twice is a no-op. (Next step if needed: a `held` state with `expires_at` and a sweeper that uses the same guarded update.)

## 4. Consistency vs availability under a partition
Single Postgres primary = CP. If the app can't reach the DB it returns 503 and `/readyz` fails closed; it never "guesses" a seat. The only cached data is each show's price and per-user limit, which are immutable, so the cache can never cause a stale decision about seat availability. Failing over to a replica would risk split-brain double-sells, so the design prefers refusing
sales to selling the same seat twice. Reads (`GET /shows`) use a separate small pool so a write storm can't starve them.

## 5. Observability: what pages me at 2am
- `db_up == 0` or `/readyz` failing >1 min (nothing can be sold).
- `rate(http_requests_total{status=~"5.."}[5m]) > 0`: the contract is zero 5xx; any is a bug or an outage.
- `rate(http_requests_total{status="429"}[1m]) > 0`: pool saturation/shedding.
- `db_write_pool_in_use` pinned at max with p99 latency climbing: DB is the bottleneck (queueing, not erroring).
- Correctness canary (not a built-in metric, so it needs a small job): a synthetic probe polling
  `GET /shows/{id}?include_seats=false` and alerting if `invariant_ok` is false, plus a periodic SQL check that
  `count(*) FROM seats WHERE status='confirmed'` equals `sum(cardinality(seats)) FROM reservations WHERE status='confirmed'`.
- Business sanity: `reservations_confirmed_total` flat during an on-sale while requests are arriving.

## 6. AI usage (directed vs decided):
**What I decided myself**
- Language and platform: I chose Python because it's the language I'm most comfortable extending live; Render because it has a free tier with managed Postgres.
- Scope and staging: I decided to build the repo with the AI's help, read it file by file, and commit it in
  dependency order (auth → observability → DB layer → API → Docker → burst → deploy → docs).
- Deployment and operations were mine: I created the Render service and database, set the
  admin token, ran the live burst, recorded the log demo, and handled environment problems
  (no Docker on my Mac at first, so I ran Postgres locally via Homebrew)

**What I directed the AI to produce**
- I gave it the assignment text. It proposed the architecture (single Postgres, row locks with
  `SELECT … FOR UPDATE` in sorted order, a unique idempotency key claimed first, a per-user advisory lock for
  the limit, guarded cancel, all-or-nothing multi-seat) and generated the code: `app/`, `burst.py`,
  Dockerfile, compose, `render.yaml`, and drafts of the README and this write-up.
- I then directed explanations, not just code: a step-by-step commit plan, a plain-language explanation of every
  file and function, and fixes for my local setup and deployment.

**What the AI got wrong, and what was caught**
- The AI ran the burst in its own sandbox before handing it to me. Two issues surfaced:
  1.  In its own sandbox run, the AI found that uvicorn's default 5-second keep-alive caused
  `ServerDisconnectedError` for clients reusing idle connections; fixed with `--timeout-keep-alive 75`.
  2. Its setup guidance suggested `ulimit -n 65535`, which macOS doesn't allow; I corrected it to 10240.

**How I verified**
- Local: the burst against Postgres installed via Homebrew ended with ALL CHECKS PASSED.
- Live: I ran the burst against the Render URL; output is in `docs/burst-live-output.txt`

## 7. What I'd do next
TTL holds + sweeper; per-show `FOR UPDATE SKIP LOCKED` seat-picker ("best available"); outbox for payment/webhook side effects; multi-instance
load test (counters are per-process, gauges are DB-derived); rate-limit `/auth/token`; partition `seats` by show; PgBouncer in transaction mode (needs `statement_cache_size=0`).
