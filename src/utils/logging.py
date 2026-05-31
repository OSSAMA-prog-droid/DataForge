import structlog
import logging
from src.config import settings

# BUG DF-03: PII fields are logged at DEBUG level.
# When LOG_LEVEL=DEBUG (common in staging/dev), every processed record is logged
# in full — including email, phone, ssn, date_of_birth, and card_last_four.
# Staging environments often share log aggregators with production.
# Any log scraping tool, Splunk index, or CloudWatch export captures this PII
# permanently, violating GDPR Article 5 (data minimisation) and CCPA.

PII_FIELDS = {"email", "phone", "ssn", "date_of_birth", "card_last_four", "ip_address"}


def configure_logging() -> None:
    logging.basicConfig(level=getattr(logging, settings.log_level.upper()))
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, settings.log_level.upper())
        ),
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)


def log_record(logger: structlog.BoundLogger, record: dict) -> None:
    # BUG DF-03: logs the full record dict including any PII fields present.
    # Fix: scrub PII_FIELDS from record before logging, or log only record ID and type.
    logger.debug("processing_record", record=record)


def log_record_error(logger: structlog.BoundLogger, record: dict, error: Exception) -> None:
    # BUG DF-03: error logs also include the full record payload.
    logger.error("record_processing_failed", record=record, error=str(error))
