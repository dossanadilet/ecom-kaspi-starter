from __future__ import annotations

from apps.api.core.s3 import get_s3_client
from apps.api.core.config import settings


def download_file(key: str, local_path: str) -> None:
    s3 = get_s3_client()
    s3.download_file(settings.s3_bucket, key, local_path)
    print(f"downloaded s3://{settings.s3_bucket}/{key} -> {local_path}")

