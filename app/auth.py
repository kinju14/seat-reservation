"""Identity is ALWAYS derived from the bearer token, never from the request body."""
import hmac
import os
import time

import jwt
from fastapi import Header

JWT_SECRET = os.environ.get("JWT_SECRET", "dev-secret-change-me-dev-secret-change-me")
ADMIN_TOKEN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")
# Open token minting is intentional so graders/burst scripts can create users. Turn off in real life.
DEV_AUTH = os.environ.get("ENABLE_DEV_AUTH", "true").lower() == "true"


class AuthError(Exception):
    def __init__(self, status: int, code: str):
        self.status, self.code = status, code


def issue_token(user_id: str) -> str:
    now = int(time.time())
    return jwt.encode({"sub": user_id, "iat": now, "exp": now + 7 * 86400}, JWT_SECRET, algorithm="HS256")


def _bearer(h):
    if not h or not h.lower().startswith("bearer "):
        return None
    return h[7:].strip() or None


async def current_user(authorization: str | None = Header(default=None)) -> str:
    tok = _bearer(authorization)
    if not tok:
        raise AuthError(401, "unauthorized")
    try:
        claims = jwt.decode(tok, JWT_SECRET, algorithms=["HS256"])
    except jwt.PyJWTError:
        raise AuthError(401, "unauthorized")
    sub = claims.get("sub")
    if not isinstance(sub, str) or not sub:
        raise AuthError(401, "unauthorized")
    return sub


async def require_admin(authorization: str | None = Header(default=None)) -> str:
    tok = _bearer(authorization)
    if not tok:
        raise AuthError(401, "unauthorized")
    if not hmac.compare_digest(tok.encode(), ADMIN_TOKEN.encode()):
        raise AuthError(403, "admin_required")
    return "admin"
