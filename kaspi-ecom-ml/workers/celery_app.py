from __future__ import annotations

import os
from celery import Celery

broker = os.getenv("REDIS_URL", "redis://redis:6379/0")
celery_app = Celery("kaspi", broker=broker, backend=broker)
celery_app.conf.update(task_serializer="json", result_serializer="json", accept_content=["json"])

