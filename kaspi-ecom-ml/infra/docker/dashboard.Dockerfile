FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY pyproject.toml /app/
RUN pip install --upgrade pip && pip install .
COPY apps /app/apps
EXPOSE 8501
CMD ["streamlit", "run", "apps/dashboard/app.py", "--server.port", "8501", "--server.headless", "true"]
