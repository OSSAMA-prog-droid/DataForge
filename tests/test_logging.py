import json
import pytest
from unittest.mock import MagicMock


# DF-03: PII fields written to logs
class TestPIILogging:
    def test_log_record_includes_pii_fields(self):
        from src.utils.logging import log_record, PII_FIELDS

        logged_events = []

        mock_logger = MagicMock()
        mock_logger.debug = lambda event, **kw: logged_events.append(kw)

        pii_record = {
            "transaction_id": "tx-001",
            "amount": 99.99,
            "email": "jane.doe@example.com",       # PII
            "phone": "+44 7700 900123",             # PII
            "ssn": "123-45-6789",                   # PII
            "merchant_id": "merch-42",
        }

        # Bug: log_record writes the full dict including PII fields
        log_record(mock_logger, pii_record)

        assert len(logged_events) == 1
        logged = logged_events[0]

        # PII is present in the log — demonstrates the bug
        assert "record" in logged
        assert logged["record"]["email"] == "jane.doe@example.com"
        assert logged["record"]["ssn"] == "123-45-6789"

        # Fix: logged dict should not contain any PII_FIELDS keys
        # assert all(k not in logged["record"] for k in PII_FIELDS)

    def test_log_record_error_includes_pii(self):
        from src.utils.logging import log_record_error

        logged_events = []
        mock_logger = MagicMock()
        mock_logger.error = lambda event, **kw: logged_events.append(kw)

        pii_record = {"transaction_id": "tx-002", "email": "victim@example.com", "phone": "555-1234"}
        log_record_error(mock_logger, pii_record, ValueError("bad amount"))

        assert logged_events[0]["record"]["email"] == "victim@example.com"
