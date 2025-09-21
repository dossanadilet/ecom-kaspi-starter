from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Dict, Any

import jwt

from .config import settings


def create_token(sub: str) -> str:
    """Create a JWT token for subject."""
    now = datetime.now(tz=timezone.utc)
    payload: Dict[str, Any] = {
        "sub": sub,
        "iat": int(now.timestamp()),
        "exp": int((now + timedelta(minutes=settings.jwt_expire_minutes)).timestamp()),
        "env": settings.env,
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm="HS256")


def decode_token(token: str) -> Dict[str, Any]:
    return jwt.decode(token, settings.jwt_secret, algorithms=["HS256"])

