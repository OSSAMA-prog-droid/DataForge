import redis as redis_lib
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# BUG DF-05: Deduplication TTL (60 seconds) is shorter than the Kafka retry window
# (5 minutes / 300 seconds). When a producer retries a failed publish after 2–5
# minutes (within Kafka's default retry window), the dedup key has already expired.
# The record is treated as a new event and processed a second time.
# At DataForge's scale this causes ~0.8% of records to be double-counted in
# aggregations — enough to corrupt financial reports by thousands of dollars.
# Fix: set TTL to at least 2× the maximum Kafka retry window (600+ seconds),
# or use the Kafka message offset as the dedup key with a longer retention.

DEDUP_TTL_SECONDS = 60  # BUG DF-05: should be >= 600 (10 minutes)
DEDUP_KEY_PREFIX = "dataforge:dedup:"


def get_redis() -> redis_lib.Redis:
    return redis_lib.from_url(settings.redis_url, decode_responses=True)


_redis: redis_lib.Redis | None = None


def get_shared_redis() -> redis_lib.Redis:
    global _redis
    if _redis is None:
        _redis = get_redis()
    return _redis


def is_duplicate(event_id: str) -> bool:
    r = get_shared_redis()
    key = f"{DEDUP_KEY_PREFIX}{event_id}"
    # SET key 1 NX EX ttl — returns True if key was newly set (not duplicate)
    result = r.set(key, "1", nx=True, ex=DEDUP_TTL_SECONDS)
    return result is None  # None means key already existed → duplicate


def mark_processed(event_id: str) -> None:
    r = get_shared_redis()
    key = f"{DEDUP_KEY_PREFIX}{event_id}"
    r.set(key, "1", ex=DEDUP_TTL_SECONDS)
