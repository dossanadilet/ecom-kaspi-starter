#!/usr/bin/env sh
set -e

python -m apps.api.startup
exec uvicorn apps.api.main:app --host 0.0.0.0 --port 8000

