FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PREFECT_API_URL=http://0.0.0.0:4200/api
WORKDIR /app
COPY pyproject.toml /app/
RUN pip install --upgrade pip && pip install .
COPY dags /app/dags
EXPOSE 4200
CMD ["prefect", "server", "start", "--host", "0.0.0.0", "--port", "4200"]

