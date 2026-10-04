"""Structured logging, Prometheus metrics, and the request-id / metrics ASGI middleware."""
import contextvars
import json
import logging
import os
import sys
import time
import uuid

from prometheus_client import Counter, Gauge, Histogram

request_id_var = contextvars.ContextVar("request_id", default="-")
_fields_var = contextvars.ContextVar("log_fields", default=None)
log = logging.getLogger("app")

# Decline reasons (also the `reason` label values on reservations_declined_total).
REASONS = ("seat_taken", "per_user_limit", "idempotent_replay", "key_conflict", "unknown_seat")

CONFIRMED = Counter("reservations_confirmed", "Reservations confirmed (committed, first-time)")
DECLINED = Counter("reservations_declined", "Reservation requests not newly confirmed, by reason", ["reason"])
CANCELLED = Counter("reservations_cancelled", "Reservations cancelled (state actually changed)")
for _r in REASONS:
    DECLINED.labels(_r)  # export zeros so dashboards/alerts see the series from t=0

SEATS_AVAILABLE = Gauge("seats_available", "Seats available (computed from DB at scrape time)", ["show_id"])
SEATS_HELD = Gauge("seats_held", "Seats held (computed from DB at scrape time)", ["show_id"])
SEATS_CONFIRMED = Gauge("seats_confirmed", "Seats confirmed (computed from DB at scrape time)", ["show_id"])
DB_UP = Gauge("db_up", "1 if the DB answered at scrape time")
POOL_IN_USE = Gauge("db_write_pool_in_use", "Write-pool connections currently checked out")
POOL_SIZE = Gauge("db_write_pool_size", "Write-pool connections currently open")
INFLIGHT = Gauge("http_requests_in_flight", "Requests currently being served")
HTTP_REQS = Counter("http_requests", "HTTP requests", ["route", "method", "status"])
HTTP_LAT = Histogram(
    "http_request_duration_seconds", "HTTP latency", ["route"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)


def annotate(**kw):
    """Attach fields to this request's access-log line (e.g. user_id, outcome)."""
    f = _fields_var.get()
    if f is not None:
        f.update(kw)


class JsonFormatter(logging.Formatter):
    def format(self, record):
        d = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created)) + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "request_id": request_id_var.get(),
        }
        d.update(getattr(record, "fields", {}))
        if record.exc_info:
            d["exc"] = self.formatException(record.exc_info)
        return json.dumps(d, default=str)


def setup_logging():
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [h]
    root.setLevel(os.environ.get("LOG_LEVEL", "INFO"))
    for n in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(n)
        lg.handlers[:] = []
        lg.propagate = True
    logging.getLogger("uvicorn.access").disabled = True


_QUIET = {"/healthz", "/readyz", "/metrics"}


class ObservabilityMiddleware:
    """Pure-ASGI (no BaseHTTPMiddleware overhead): request id, access log, HTTP metrics."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        hdrs = dict(scope["headers"])
        rid = (hdrs.get(b"x-request-id") or b"").decode("latin-1")[:64] or uuid.uuid4().hex
        fields = {}
        t_rid = request_id_var.set(rid)
        t_f = _fields_var.set(fields)
        status = 500
        t0 = time.perf_counter()
        INFLIGHT.inc()

        async def send_wrapper(msg):
            nonlocal status
            if msg["type"] == "http.response.start":
                status = msg["status"]
                msg = {**msg, "headers": [*msg.get("headers", []), (b"x-request-id", rid.encode("latin-1"))]}
            await send(msg)

        try:
            await self.app(scope, receive, send_wrapper)
        except Exception:
            log.exception("unhandled_exception")
            raise
        finally:
            INFLIGHT.dec()
            dur = time.perf_counter() - t0
            route = getattr(scope.get("route"), "path", None) or "unmatched"
            HTTP_REQS.labels(route, scope["method"], str(status)).inc()
            HTTP_LAT.labels(route).observe(dur)
            path = scope["path"]
            lvl = logging.DEBUG if path in _QUIET else (logging.ERROR if status >= 500 else logging.INFO)
            log.log(lvl, "request", extra={"fields": {
                "method": scope["method"], "path": path, "route": route,
                "status": status, "duration_ms": round(dur * 1000, 1), **fields}})
            request_id_var.reset(t_rid)
            _fields_var.reset(t_f)
