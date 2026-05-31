import json
from confluent_kafka import Producer
from src.config import settings
from src.utils.logging import get_logger

logger = get_logger(__name__)

# BUG DF-06: Producer configured with acks='1'.
# With acks='1', the broker only waits for the partition leader to acknowledge
# the write before confirming to the producer. If the leader crashes before
# the message is replicated to followers, the message is permanently lost —
# with no error returned to the producer.
# At DataForge's ingest rate of ~50,000 events/min, a 30-second leader election
# window loses ~25,000 records silently. The pipeline shows no errors.
# Fix: set acks='all' (or acks=-1) and enable.idempotence=True.


def get_producer() -> Producer:
    return Producer({
        "bootstrap.servers": settings.kafka_bootstrap_servers,
        "acks": "1",                    # BUG DF-06: should be "all"
        "retries": 3,
        "retry.backoff.ms": 100,
        # missing: "enable.idempotence": True
        # missing: "max.in.flight.requests.per.connection": 1
    })


_producer: Producer | None = None


def get_shared_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = get_producer()
    return _producer


def publish(topic: str, key: str, value: dict) -> None:
    producer = get_shared_producer()
    producer.produce(
        topic=topic,
        key=key.encode("utf-8"),
        value=json.dumps(value).encode("utf-8"),
        callback=_delivery_callback,
    )
    producer.poll(0)


def flush() -> None:
    get_shared_producer().flush()


def _delivery_callback(err, msg) -> None:
    if err:
        logger.error("kafka_delivery_failed", error=str(err), topic=msg.topic())
    else:
        logger.debug("kafka_delivery_ok", topic=msg.topic(), partition=msg.partition())
