"""Auth service — bcrypt password hashing + JWT issuance/verification."""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt
import jwt

SECRET = os.environ.get("JWT_SECRET") or os.urandom(32).hex()
ALGO = "HS256"
TOKEN_TTL = timedelta(hours=24)


def hash_password(plain: str) -> str:
    return bcrypt.hashpw(plain.encode(), bcrypt.gensalt(rounds=12)).decode()


def verify_password(plain: str, hashed: str) -> bool:
    return bcrypt.checkpw(plain.encode(), hashed.encode())


def sign_token(user_id: int, email: str) -> str:
    now = datetime.now(timezone.utc)
    return jwt.encode(
        {
            "sub": str(user_id),
            "email": email,
            "iat": int(now.timestamp()),
            "exp": int((now + TOKEN_TTL).timestamp()),
        },
        SECRET,
        algorithm=ALGO,
    )


def decode_token(token: str) -> dict[str, Any]:
    """Raises jwt.ExpiredSignatureError / jwt.InvalidTokenError on bad tokens."""
    return jwt.decode(token, SECRET, algorithms=[ALGO])
