"""All SQL lives here. The atomic decisions are `_reserve_tx` and `cancel`.

Lock order inside every transaction (this is what makes deadlock impossible):
    1. idempotency-key row   (unique index on reservations(user_id, idempotency_key))
    2. advisory lock         (per show+user; serialises that user's limit check)
    3. seat rows             (SELECT ... ORDER BY label FOR UPDATE, always sorted)
"""
import asyncio
import hashlib
import json
import logging
import os
import random
import uuid

import asyncpg

log = logging.getLogger("app")

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/seats")
WRITE_POOL_MAX = int(os.environ.get("DB_POOL_MAX", "20"))
READ_POOL_MAX = int(os.environ.get("DB_READ_POOL_MAX", "4"))
# Generous on purpose: under a stampede we QUEUE for a connection instead of erroring.
ACQUIRE_TIMEOUT = float(os.environ.get("DB_ACQUIRE_TIMEOUT", "90"))
MAX_TX_RETRIES = 6

write_pool: asyncpg.Pool | None = None  # reserve / cancel / create-show
read_pool: asyncpg.Pool | None = None   # GET /shows, /readyz, /metrics: never starved by a burst


class Overloaded(Exception):
    """Could not get a DB connection in time -> 429 (a 4xx), never a 5xx."""


SCHEMA = """
CREATE TABLE IF NOT EXISTS shows (
  id             UUID PRIMARY KEY,
  name           TEXT NOT NULL,
  price_paise    BIGINT NOT NULL CHECK (price_paise >= 0),
  per_user_limit INT NOT NULL DEFAULT 4 CHECK (per_user_limit > 0),
  total_seats    INT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS reservations (
  id              UUID PRIMARY KEY,
  show_id         UUID NOT NULL REFERENCES shows(id),
  user_id         TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash    TEXT NOT NULL,
  status          TEXT NOT NULL CHECK (status IN ('pending','confirmed','declined','cancelled')),
  decline_reason  TEXT,
  unavailable     TEXT[],
  seats           TEXT[] NOT NULL,
  amount_paise    BIGINT NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT reservations_idem UNIQUE (user_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS seats (
  show_id        UUID NOT NULL REFERENCES shows(id),
  label          TEXT NOT NULL,
  pos            INT NOT NULL,
  status         TEXT NOT NULL DEFAULT 'available' CHECK (status IN ('available','held','confirmed')),
  reservation_id UUID REFERENCES reservations(id),
  user_id        TEXT,
  PRIMARY KEY (show_id, label)
);
CREATE INDEX IF NOT EXISTS seats_user_idx ON seats (show_id, user_id) WHERE user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS seats_res_idx ON seats (reservation_id) WHERE reservation_id IS NOT NULL;
"""

DECLINE_HTTP = {"seat_taken": 409, "per_user_limit": 409, "unknown_seat": 404}
DECLINE_MSG = {
    "seat_taken": "one or more requested seats are already taken (all-or-nothing: nothing was reserved)",
    "per_user_limit": "reservation would exceed the per-user seat limit for this show",
    "unknown_seat": "one or more requested seats do not exist in this show",
}


# ---------------------------------------------------------------- lifecycle
async def connect(retries: int = 30):
    global write_pool, read_pool
    for i in range(retries):
        try:
            write_pool = await asyncpg.create_pool(
                DATABASE_URL, min_size=2, max_size=WRITE_POOL_MAX, command_timeout=60)
            break
        except (OSError, asyncpg.PostgresError) as e:
            log.warning("db_not_ready", extra={"fields": {"attempt": i + 1, "error": str(e)}})
            await asyncio.sleep(1)
    else:
        raise RuntimeError("database unreachable at startup")
    read_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=READ_POOL_MAX, command_timeout=30)
    async with write_pool.acquire() as c:  # advisory lock so parallel instances don't race on DDL
        await c.execute("SELECT pg_advisory_lock(727274)")
        try:
            await c.execute(SCHEMA)
        finally:
            await c.execute("SELECT pg_advisory_unlock(727274)")


async def close():
    for p in (write_pool, read_pool):
        if p:
            await asyncio.wait_for(p.close(), 10)


# ---------------------------------------------------------------- shows
_show_cache: dict[str, tuple[int, int]] = {}  # price & limit are immutable -> safe to cache
_show_locks: dict[str, asyncio.Lock] = {}


async def get_show(show_id: str):
    """(price_paise, per_user_limit) or None. Single-flight so a stampede costs one query."""
    hit = _show_cache.get(show_id)
    if hit:
        return hit
    lock = _show_locks.setdefault(show_id, asyncio.Lock())
    try:
        async with lock:
            if show_id not in _show_cache:
                async with read_pool.acquire(timeout=ACQUIRE_TIMEOUT) as c:
                    row = await c.fetchrow("SELECT price_paise, per_user_limit FROM shows WHERE id=$1", uuid.UUID(show_id))
                if row:
                    _show_cache[show_id] = (row["price_paise"], row["per_user_limit"])
            return _show_cache.get(show_id)
    finally:
        _show_locks.pop(show_id, None)


async def create_show(name, seats, price_paise, per_user_limit):
    sid = uuid.uuid4()
    async with write_pool.acquire(timeout=ACQUIRE_TIMEOUT) as c:
        async with c.transaction():
            await c.execute(
                "INSERT INTO shows (id, name, price_paise, per_user_limit, total_seats) VALUES ($1,$2,$3,$4,$5)",
                sid, name, price_paise, per_user_limit, len(seats))
            await c.execute(
                "INSERT INTO seats (show_id, label, pos) "
                "SELECT $1::uuid, t.label, t.ord FROM unnest($2::text[]) WITH ORDINALITY AS t(label, ord)",
                sid, seats)
    return await show_state(str(sid), True)


async def show_state(show_id: str, include_seats: bool = True):
    sid = uuid.UUID(show_id)
    async with read_pool.acquire(timeout=ACQUIRE_TIMEOUT) as c:
        show = await c.fetchrow("SELECT * FROM shows WHERE id=$1", sid)
        if show is None:
            return None
        if include_seats:
            # one statement => one snapshot => counts derived from it are mutually consistent
            rows = await c.fetch("SELECT label, status FROM seats WHERE show_id=$1 ORDER BY pos", sid)
            by = {}
            for r in rows:
                by[r["status"]] = by.get(r["status"], 0) + 1
        else:
            rows = None
            by = {r["status"]: r["n"] for r in await c.fetch(
                "SELECT status, count(*) AS n FROM seats WHERE show_id=$1 GROUP BY status", sid)}
    counts = {k: by.get(k, 0) for k in ("available", "held", "confirmed")}
    out = {
        "id": str(show["id"]), "name": show["name"], "price_paise": show["price_paise"],
        "per_user_limit": show["per_user_limit"], "total_seats": show["total_seats"],
        "counts": counts,
        "invariant_ok": sum(counts.values()) == show["total_seats"],
    }
    if rows is not None:
        out["seats"] = [{"seat": r["label"], "status": r["status"]} for r in rows]
    return out


async def seat_gauges(limit: int):
    async with read_pool.acquire(timeout=5) as c:
        return await c.fetch(
            """SELECT show_id,
                      count(*) FILTER (WHERE status='available') AS available,
                      count(*) FILTER (WHERE status='held')      AS held,
                      count(*) FILTER (WHERE status='confirmed') AS confirmed
               FROM seats
               WHERE show_id IN (SELECT id FROM shows ORDER BY created_at DESC LIMIT $1)
               GROUP BY show_id""", limit)


async def ping():
    async with read_pool.acquire(timeout=2) as c:
        await c.fetchval("SELECT 1")


# ---------------------------------------------------------------- reserve
def _view(row) -> dict:
    return {"reservation_id": str(row["id"]), "show_id": str(row["show_id"]), "user_id": row["user_id"],
            "seats": list(row["seats"]), "amount_paise": row["amount_paise"], "status": row["status"]}


def _decline_body(reason: str, unavailable) -> dict:
    return {"error": reason, "message": DECLINE_MSG[reason], "status": "declined",
            "unavailable_seats": list(unavailable or [])}


async def reserve(show_id: str, user_id: str, key: str, seats: list[str], price: int, limit: int):
    """Returns (http_status, body, kind, replay). kind is 'confirmed' or a metrics reason."""
    ordered = sorted(seats)
    req_hash = hashlib.sha256(json.dumps({"show": show_id, "seats": ordered}).encode()).hexdigest()
    for attempt in range(MAX_TX_RETRIES):
        try:
            async with write_pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
                async with conn.transaction():  # READ COMMITTED; commit happens on exit
                    return await _reserve_tx(conn, show_id, user_id, key, req_hash, seats, ordered, price, limit)
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            await asyncio.sleep(random.uniform(0.005, 0.03) * (attempt + 1))
        except asyncio.TimeoutError:
            raise Overloaded()
    raise Overloaded()


async def _decline(conn, rid, reason, unavailable):
    # The decline is COMMITTED with the claimed key, so a retry replays this exact outcome.
    await conn.execute("UPDATE reservations SET status='declined', decline_reason=$2, unavailable=$3 WHERE id=$1",
                       rid, reason, unavailable)
    return DECLINE_HTTP[reason], _decline_body(reason, unavailable), reason, False


async def _reserve_tx(conn, show_id, user_id, key, req_hash, seats, ordered, price, limit):
    sid, rid = uuid.UUID(show_id), uuid.uuid4()

    # (1) Claim the idempotency key. Concurrent same-key requests block on the unique index
    #     until the first transaction commits, then see the conflict -> exactly-once.
    claimed = await conn.fetchval(
        """INSERT INTO reservations (id, show_id, user_id, idempotency_key, request_hash, status, seats, amount_paise)
           VALUES ($1,$2,$3,$4,$5,'pending',$6,$7)
           ON CONFLICT (user_id, idempotency_key) DO NOTHING RETURNING id""",
        rid, sid, user_id, key, req_hash, seats, price * len(seats))
    if claimed is None:
        ex = await conn.fetchrow("SELECT * FROM reservations WHERE user_id=$1 AND idempotency_key=$2", user_id, key)
        if ex["request_hash"] != req_hash:
            return (409, {"error": "idempotency_key_conflict",
                          "message": "this idempotency key was already used with a different request"},
                    "key_conflict", False)
        if ex["status"] == "declined":
            r = ex["decline_reason"]
            return DECLINE_HTTP[r], _decline_body(r, ex["unavailable"]), "idempotent_replay", True
        return (201 if ex["status"] == "confirmed" else 200), _view(ex), "idempotent_replay", True

    # (2) Serialise this user's attempts on this show, THEN count (fresh snapshot per statement).
    await conn.execute("SELECT pg_advisory_xact_lock(hashtextextended($1::text, 0))", f"{show_id}:{user_id}")
    held = await conn.fetchval(
        "SELECT count(*) FROM seats WHERE show_id=$1 AND user_id=$2 AND status IN ('held','confirmed')", sid, user_id)
    if held + len(seats) > limit:
        return await _decline(conn, rid, "per_user_limit", [])

    # (3) Lock the requested seats in a deterministic order, then decide. All-or-nothing.
    rows = await conn.fetch(
        "SELECT label, status FROM seats WHERE show_id=$1 AND label = ANY($2::text[]) ORDER BY label FOR UPDATE",
        sid, ordered)
    if len(rows) != len(ordered):
        known = {r["label"] for r in rows}
        return await _decline(conn, rid, "unknown_seat", [s for s in ordered if s not in known])
    taken = [r["label"] for r in rows if r["status"] != "available"]
    if taken:
        return await _decline(conn, rid, "seat_taken", taken)

    await conn.execute(
        """WITH s AS (UPDATE seats SET status='confirmed', reservation_id=$3, user_id=$4
                      WHERE show_id=$1 AND label = ANY($2::text[]))
           UPDATE reservations SET status='confirmed' WHERE id=$3""",
        sid, ordered, rid, user_id)
    return (201, {"reservation_id": str(rid), "show_id": show_id, "user_id": user_id, "seats": seats,
                  "amount_paise": price * len(seats), "status": "confirmed"}, "confirmed", False)


# ---------------------------------------------------------------- cancel
async def cancel(reservation_id: str, user_id: str):
    """Returns (http_status, body, changed)."""
    rid = uuid.UUID(reservation_id)
    for attempt in range(MAX_TX_RETRIES):
        try:
            async with write_pool.acquire(timeout=ACQUIRE_TIMEOUT) as conn:
                async with conn.transaction():
                    r = await conn.fetchrow("SELECT * FROM reservations WHERE id=$1 FOR UPDATE", rid)
                    if r is None:
                        return 404, {"error": "reservation_not_found"}, False
                    if r["user_id"] != user_id:
                        return 403, {"error": "forbidden", "message": "only the owner can cancel"}, False
                    if r["status"] == "cancelled":
                        return 200, _view(r), False  # idempotent no-op; touches NO seats
                    if r["status"] != "confirmed":
                        return 409, {"error": "not_cancellable", "message": f"reservation is {r['status']}"}, False
                    await conn.fetch(
                        "SELECT label FROM seats WHERE show_id=$1 AND reservation_id=$2 ORDER BY label FOR UPDATE",
                        r["show_id"], rid)
                    # Guarded on reservation_id: can only free seats THIS reservation still owns.
                    await conn.execute(
                        """WITH s AS (UPDATE seats SET status='available', reservation_id=NULL, user_id=NULL
                                      WHERE show_id=$1 AND reservation_id=$2)
                           UPDATE reservations SET status='cancelled' WHERE id=$2""", r["show_id"], rid)
                    body = _view(r)
                    body["status"] = "cancelled"
                    return 200, body, True
        except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError):
            await asyncio.sleep(random.uniform(0.005, 0.03) * (attempt + 1))
        except asyncio.TimeoutError:
            raise Overloaded()
    raise Overloaded()
