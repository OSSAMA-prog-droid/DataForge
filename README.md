# DataForge

Python + Kafka + Spark data pipeline platform. Ingests data from operational databases and event streams, transforms with Apache Spark, and loads into a PostgreSQL data warehouse.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
docker-compose up -d postgres kafka redis
alembic upgrade head
uvicorn src.api.main:app --reload
```

## Test

```bash
pytest tests/
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql+asyncpg://...` | Async DB URL |
| `SYNC_DATABASE_URL` | `postgresql://...` | Sync DB URL (psycopg2) |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `REDIS_URL` | `redis://localhost:6379` | Redis for dedup + watermarks |
| `PIPELINE_SCHEDULE_INTERVAL_MINUTES` | `15` | Scheduler interval |
