FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY pyproject.toml /app/
RUN pip install --upgrade pip && pip install .
COPY workers /app/workers
COPY apps /app/apps
CMD ["celery", "-A", "workers.celery_app:celery_app", "worker", "-l", "INFO"]
