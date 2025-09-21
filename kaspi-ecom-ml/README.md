# Kaspi E-commerce ML (MVP v1.0)

Minimal but runnable platform for Kaspi marketplace analytics: scraping + ETL + ML + API + Streamlit dashboard.

Highlights
- FastAPI backend with JWT, Prometheus `/metrics`, Postgres + S3 clients
- Streamlit dashboard (русский UI, графики по SKU, фильтр рекомендаций, Telegram действия)
- ETL + скрапинг Kaspi (Playwright) с выгрузкой в `data/latest/market_snapshot.csv`
- Prefect flow: nightly ETL → рекомендации → аномалии → Telegram дайджест
- Dockerized stack: Postgres, Redis, MinIO, Celery worker, Prefect, Prometheus, Grafana, Loki

## Quickstart

1) Copy env
```
cp .env.example .env
```

2) Build and run stack
```
docker compose up -d --build
```

3) Initialize DB and seed demo
```
docker compose exec api alembic -c db/alembic.ini upgrade head
docker compose exec postgres psql -U user -d kaspi -f /app/db/sql/seed_demo.sql
```

4) Open
- API: http://localhost:8000/docs
- Dashboard: http://localhost:8501

Demo scenario
- Call `GET /products/{sku}/price-reco` on a seeded SKU to get a price recommendation with expected qty/profit.

## Daily ETL & Kaspi scraping

- Быстрый прогон локально:
  ```bash
  python etl/run_etl.py --config etl/config.quick.yaml
  ```
- Полный прогон по `etl/config.yaml` (смартфоны + бренды):
  ```bash
  python etl/run_etl.py
  ```
- После запуска – свежие CSV:
  - `data/latest/market_snapshot.csv`
  - `data/daily/market_snapshot_YYYY-MM-DD.csv`
- При наличии `TG_BOT_TOKEN`/`TG_CHAT_ID` в окружении ETL отправит сводку + CSV в Telegram.

### GitHub Actions
- Workflow `.github/workflows/etl.yml` запускается каждый день в 00:00 UTC (06:00 Asia/Almaty).
- Требуемые секреты репозитория:
  - `TG_BOT_TOKEN` — токен телеграм-бота.
  - `TG_CHAT_ID` — chat_id группы/канала/пользователя.
- Артефакт `kaspi-market-snapshot` содержит актуальные CSV; при запуске с ветки `main` данные коммитятся в repo.

## Acceptance criteria
- `docker compose up -d` brings up services; `GET /health` returns ok.
- With seed data, dashboard shows basic tables/graphs.
- `GET /products/{sku}/price-reco` returns JSON with fields: `reco_price, expected_qty, expected_profit, explain, model_ver`.
- `dags/flows.py` can be invoked manually (mock/demo) to write `demand_forecast/price_reco` records.

## Telegram интеграция
- Настройте переменные в `.env` (`TG_BOT_TOKEN`, `TG_CHAT_ID`, `TG_DIGEST_ENABLED`, `TG_DIGEST_HOUR`, `TZ`).
- В панели «Администрирование»: тестовое сообщение, отправка рекомендаций (CSV), запуск дайджеста, запуск аномалий.
- Nightly flow присылает top‑5 рекомендаций + CSV в указанный чат.

## Repo layout
See directory tree under this folder. All modules have docstrings and type hints where public.
