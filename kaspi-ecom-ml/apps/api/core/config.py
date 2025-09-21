from __future__ import annotations

import os
from pydantic import BaseModel


class Settings(BaseModel):
    database_url: str = os.getenv("DATABASE_URL", "postgresql+psycopg://user:pass@postgres:5432/kaspi")
    redis_url: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    s3_endpoint: str = os.getenv("S3_ENDPOINT", "http://minio:9000")
    s3_bucket: str = os.getenv("S3_BUCKET", "kaspi-raw")
    s3_access_key: str = os.getenv("S3_ACCESS_KEY", "minioadmin")
    s3_secret_key: str = os.getenv("S3_SECRET_KEY", "minioadmin")
    jwt_secret: str = os.getenv("JWT_SECRET", "change_me")
    jwt_expire_minutes: int = int(os.getenv("JWT_EXPIRE_MINUTES", "60"))
    env: str = os.getenv("ENV", "dev")


settings = Settings()

