FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app
COPY pyproject.toml /app/
RUN pip install --upgrade pip && pip install .

COPY apps /app/apps
COPY db /app/db
COPY dags /app/dags
COPY services /app/services
COPY infra/docker/start-api.sh /app/start-api.sh
RUN chmod +x /app/start-api.sh

EXPOSE 8000
CMD ["/app/start-api.sh"]
