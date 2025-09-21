from __future__ import annotations

import os
from pathlib import Path
from apps.api.core.s3 import get_s3_client
from apps.api.core.config import settings


def upload_file(local_path: str, key: str) -> None:
    s3 = get_s3_client()
    s3.upload_file(local_path, settings.s3_bucket, key)
    print(f"uploaded {local_path} -> s3://{settings.s3_bucket}/{key}")


if __name__ == "__main__":
    p = Path(__file__).resolve()
    print(p)

