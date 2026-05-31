import json
from datetime import datetime
from confluent_kafka import Producer
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

DLQ_TOPIC = "dataforge.dlq"

# BUG DF-07: The DLQ topic is written to on every processing failure, but there
# is NO consumer for it anywhere in the codebase. Failed records accumulate
# indefinitely in the Kafka topic. With default Kafka retention of 7 days and
# DataForge's error rate of ~0.3%, the DLQ contains ~100,000 unprocessed records
# after a week — all silently dropped when the retention window expires.
# There is no alerting, no dashboard, and no reprocessing mechanism.
# Fix: implement a DLQ consumer that retries records with exponential backoff,
# alerts on DLQ depth exceeding a threshold, and provides a manual reprocess API.


_dlq_producer: Producer | None = None


def _get_dlq_producer() -> Producer:
    global _dlq_producer
    if _dlq_producer is None:
        _dlq_producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "acks": "1",
        })
    return _dlq_producer


def send_to_dlq(record: dict, error: Exception, source_topic: str) -> None:
    producer = _get_dlq_producer()
    payload = {
        "original_record": record,
        "error": str(error),
        "error_type": type(error).__name__,
        "source_topic": source_topic,
        "failed_at": datetime.utcnow().isoformat(),
        # BUG DF-07: retry_count is tracked but nobody reads it — no reprocessor exists
        "retry_count": 0,
    }
    producer.produce(
        topic=DLQ_TOPIC,
        value=json.dumps(payload).encode("utf-8"),
    )
    producer.poll(0)
    logger.warning(
        "record_sent_to_dlq",
        source_topic=source_topic,
        error=str(error),
    )


# BUG DF-07: This function is defined but never called anywhere in the codebase.
# It represents the consumer that should exist but doesn't.
def drain_dlq() -> None:  # noqa: dead code
    raise NotImplementedError(
        "DLQ consumer not implemented — failed records are never reprocessed"
    )
