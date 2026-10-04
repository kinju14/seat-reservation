import asyncio
import logging
import os
import uuid
from contextlib import asynccontextmanager
from typing import Annotated

import asyncpg
from fastapi import Depends, FastAPI, Header
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints

from . import db, obs
from .auth import DEV_AUTH, AuthError, current_user, issue_token, require_admin

obs.setup_logging()
log = logging.getLogger("app")
METRICS_SHOW_LIMIT = int(os.environ.get("METRICS_SHOW_LIMIT", "20"))


@asynccontextmanager
async def lifespan(app):
    await db.connect()
    log.info("started", extra={"fields": {"write_pool_max": db.WRITE_POOL_MAX}})
    yield
    await db.close()


app = FastAPI(title="seat-reservation", lifespan=lifespan)
app.add_middleware(obs.ObservabilityMiddleware)

Seat = Annotated[str, StringConstraints(min_length=1, max_length=32)]
UserId = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9_.@-]{1,64}$")]


class ShowIn(BaseModel):
    model_config = ConfigDict(extra="ignore")
    name: str = Field(min_length=1, max_length=200)
    seats: list[Seat] = Field(min_length=1, max_length=100_000)
    price_paise: StrictInt = Field(ge=0)  # integers only: 250.5 is rejected, never rounded
    per_user_limit: StrictInt = Field(default=4, ge=1, le=100)


class ReserveIn(BaseModel):
    # extra="ignore": a spoofed `user_id` in the body is simply never read.
    model_config = ConfigDict(extra="ignore")
    seats: list[Seat] = Field(min_length=1, max_length=50)
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


class TokenIn(BaseModel):
    user_id: UserId


def err(status, code, message, **extra):
    return JSONResponse({"error": code, "message": message, **extra}, status_code=status)


def parse_uuid(s):
    try:
        return str(uuid.UUID(s))
    except ValueError:
        return None


# ------------------------------------------------------------ error handling
@app.exception_handler(AuthError)
async def _auth_err(_, e: AuthError):
    return err(e.status, e.code, "authentication/authorization failed")


@app.exception_handler(db.Overloaded)
async def _overloaded(_, e):
    return JSONResponse({"error": "overloaded", "message": "try again shortly"}, status_code=429,
                        headers={"Retry-After": "1"})


async def _dep_down(_, e):
    return err(503, "dependency_unavailable", "database unavailable")


for _cls in (asyncpg.PostgresError, asyncpg.InterfaceError, OSError):
    app.add_exception_handler(_cls, _dep_down)


@app.exception_handler(Exception)
async def _unhandled(_, e):
    return err(500, "internal_error", "internal error")


# ------------------------------------------------------------ auth (dev)
@app.post("/auth/token")
async def mint_token(body: TokenIn):
    if not DEV_AUTH:
        return err(404, "not_found", "dev auth disabled")
    return {"token": issue_token(body.user_id), "user_id": body.user_id}


# ------------------------------------------------------------ shows
@app.post("/shows", status_code=201)
async def create_show(body: ShowIn, _admin: str = Depends(require_admin)):
    if len(set(body.seats)) != len(body.seats):
        return err(400, "duplicate_seats", "seat labels must be unique")
    show = await db.create_show(body.name, body.seats, body.price_paise, body.per_user_limit)
    return JSONResponse(show, status_code=201)


@app.get("/shows/{show_id}")
async def get_show(show_id: str, include_seats: bool = True):
    sid = parse_uuid(show_id)
    state = await db.show_state(sid, include_seats) if sid else None
    if state is None:
        return err(404, "show_not_found", "no such show")
    return state


@app.post("/shows/{show_id}/reserve")
async def reserve(show_id: str, body: ReserveIn, user: str = Depends(current_user),
                  idempotency_key: str | None = Header(default=None)):
    sid = parse_uuid(show_id)
    show = await db.get_show(sid) if sid else None
    if show is None:
        return err(404, "show_not_found", "no such show")
    key = body.idempotency_key or idempotency_key
    if body.idempotency_key and idempotency_key and body.idempotency_key != idempotency_key:
        return err(400, "idempotency_key_mismatch", "header and body idempotency keys differ")
    if not key or len(key) > 200:
        return err(400, "idempotency_key_required", "send an Idempotency-Key header or idempotency_key field (<=200 chars)")
    if len(set(body.seats)) != len(body.seats):
        return err(400, "duplicate_seats", "seat labels must be unique within a request")

    price, limit = show
    status, payload, kind, replay = await db.reserve(sid, user, key, body.seats, price, limit)
    # Metrics are bumped AFTER db.reserve returns, i.e. after COMMIT succeeded.
    if kind == "confirmed":
        obs.CONFIRMED.inc()
    else:
        obs.DECLINED.labels(kind).inc()
    obs.annotate(user_id=user, show_id=sid, n_seats=len(body.seats), outcome=kind)
    return JSONResponse(payload, status_code=status, headers={"Idempotent-Replay": "true"} if replay else None)


@app.post("/reservations/{reservation_id}/cancel")
async def cancel(reservation_id: str, user: str = Depends(current_user)):
    rid = parse_uuid(reservation_id)
    if rid is None:
        return err(404, "reservation_not_found", "no such reservation")
    status, payload, changed = await db.cancel(rid, user)
    if changed:
        obs.CANCELLED.inc()
    obs.annotate(user_id=user, reservation_id=rid, outcome="cancelled" if changed else f"cancel_{status}")
    return JSONResponse(payload, status_code=status)


# ------------------------------------------------------------ health & metrics
@app.get("/healthz")
async def healthz():
    return {"status": "ok"}  # liveness: process is up. Deliberately does NOT touch the DB.


@app.get("/readyz")
async def readyz():
    try:  # readiness: fails CLOSED. Uses the read pool so a write-side stampede can't make us "unready".
        await asyncio.wait_for(db.ping(), 2.5)
        return {"status": "ready", "db": "up"}
    except Exception:
        return JSONResponse({"status": "unavailable", "db": "down"}, status_code=503)


@app.get("/metrics")
async def metrics():
    try:
        rows = await db.seat_gauges(METRICS_SHOW_LIMIT)
        up = 1
    except Exception:
        rows, up = [], 0
    obs.DB_UP.set(up)
    if db.write_pool:
        obs.POOL_SIZE.set(db.write_pool.get_size())
        obs.POOL_IN_USE.set(db.write_pool.get_size() - db.write_pool.get_idle_size())
    if up:  # no await between clear() and set(): the gauge family is swapped atomically
        for g in (obs.SEATS_AVAILABLE, obs.SEATS_HELD, obs.SEATS_CONFIRMED):
            g.clear()
        for r in rows:
            sid = str(r["show_id"])
            obs.SEATS_AVAILABLE.labels(sid).set(r["available"])
            obs.SEATS_HELD.labels(sid).set(r["held"])
            obs.SEATS_CONFIRMED.labels(sid).set(r["confirmed"])
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
