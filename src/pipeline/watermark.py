from datetime import datetime
import redis as redis_lib
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# BUG DF-11: Watermark uses datetime.now() — local server time, not UTC.
# On UTC+0 servers (Railway, most AWS regions) this is invisible.
# DataForge has customers whose Spark clusters run in UTC+5 (Pakistan) and
# UTC+8 (Singapore). For those tenants, hourly aggregation windows are shifted
# by 5–8 hours — a transaction at 11:50 PM local appears in the next day's
# 00:00 UTC window, making daily reports show wrong totals.
# Fix: replace datetime.now() with datetime.utcnow() throughout, or use
# timezone-aware datetimes: datetime.now(tz=timezone.utc).

WATERMARK_KEY_PREFIX = "dataforge:watermark:"


def get_redis() -> redis_lib.Redis:
    return redis_lib.from_url(settings.redis_url, decode_responses=True)


def get_watermark(pipeline_id: str) -> datetime:
    r = get_redis()
    key = f"{WATERMARK_KEY_PREFIX}{pipeline_id}"
    raw = r.get(key)
    if raw:
        return datetime.fromisoformat(raw)
    # First run — default to 24 hours ago
    # BUG DF-11: datetime.now() uses local server time
    return datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)


def set_watermark(pipeline_id: str, watermark: datetime) -> None:
    r = get_redis()
    key = f"{WATERMARK_KEY_PREFIX}{pipeline_id}"
    r.set(key, watermark.isoformat())
    logger.debug("watermark_updated", pipeline_id=pipeline_id, watermark=watermark.isoformat())


def advance_watermark(pipeline_id: str) -> datetime:
    # BUG DF-11: datetime.now() — timezone-naive, wrong for non-UTC deployments
    new_watermark = datetime.now()
    set_watermark(pipeline_id, new_watermark)
    return new_watermark
