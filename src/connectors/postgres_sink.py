import psycopg2
from psycopg2 import pool
from typing import Iterator
from contextlib import contextmanager
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# BUG DF-10: Connection pool has no upper bound (maxconn not set meaningfully).
# PostgreSQL default max_connections is 100. DataForge runs 8 pipeline workers,
# each creating its own pool. Under peak load (end-of-day batch), all 8 workers
# open connections simultaneously. With no pool limit, each worker can open up to
# the psycopg2 default maximum — exhausting the DB's connection slots.
# Other services (the API, Spark jobs, monitoring) then fail with
# "FATAL: remaining connection slots are reserved for non-replication superuser connections".
# Fix: set maxconn=10 per worker and ensure total workers × maxconn < DB max_connections.

_pool: pool.ThreadedConnectionPool | None = None


def _get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=50,   # BUG DF-10: 50 connections per worker × 8 workers = 400 > DB limit
            dsn=settings.sync_database_url,
        )
    return _pool


@contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
    conn = _get_pool().getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)


def bulk_insert(table: str, records: list[dict]) -> int:
    if not records:
        return 0

    columns = list(records[0].keys())
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join(columns)
    query = f"INSERT INTO {table} ({col_names}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"

    with get_connection() as conn:
        with conn.cursor() as cur:
            # BUG DF-10: no rate limiting or batching — inserts all records at once
            # under peak load this holds the connection open for minutes
            rows = [tuple(r[c] for c in columns) for r in records]
            cur.executemany(query, rows)
            return cur.rowcount
