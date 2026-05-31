import pytest
from unittest.mock import MagicMock, patch, call


# DF-01: auto.offset.reset='latest' skips messages during downtime
class TestOffsetReset:
    def test_consumer_configured_with_latest_offset(self):
        with patch("confluent_kafka.Consumer") as MockConsumer:
            from src.kafka.consumer import get_consumer
            get_consumer("test-group", ["test-topic"])
            config = MockConsumer.call_args[0][0]
            # Bug: consumer uses 'latest' — messages during downtime are skipped
            assert config["auto.offset.reset"] == "latest"
            # Fix: should be "earliest" so the consumer resumes from committed offset
            assert config["auto.offset.reset"] != "earliest"

    def test_messages_during_downtime_are_skipped(self):
        # Scenario:
        # 1. Consumer running — processes offsets 0–1000
        # 2. Consumer crashes / restarts — misses offsets 1001–1500 (produced during downtime)
        # 3. Consumer restarts with latest — resumes at 1501, skipping 1001–1500 permanently
        # With earliest + committed offsets, it would resume at 1001
        assert True  # documents the bug scenario


# DF-02: No schema validation — one bad record crashes the entire consumer
class TestSchemaValidation:
    def test_malformed_record_propagates_exception(self):
        from src.kafka.consumer import run_consumer

        call_count = {"n": 0}

        def handler(record: dict) -> None:
            call_count["n"] += 1
            if record.get("corrupt"):
                raise ValueError("missing required field: transaction_id")

        mock_msg_good = MagicMock()
        mock_msg_good.error.return_value = None
        mock_msg_good.value.return_value = b'{"transaction_id": "tx1", "amount": 99.99}'

        mock_msg_bad = MagicMock()
        mock_msg_bad.error.return_value = None
        mock_msg_bad.value.return_value = b'{"corrupt": true}'

        mock_msg_good2 = MagicMock()
        mock_msg_good2.error.return_value = None
        mock_msg_good2.value.return_value = b'{"transaction_id": "tx2", "amount": 50.00}'

        messages = [mock_msg_good, mock_msg_bad, mock_msg_good2, KeyboardInterrupt()]

        with patch("confluent_kafka.Consumer") as MockConsumer:
            instance = MockConsumer.return_value
            instance.subscribe = MagicMock()
            instance.close = MagicMock()

            poll_calls = iter(messages)
            def side_effect(timeout):
                val = next(poll_calls)
                if isinstance(val, type) and issubclass(val, BaseException):
                    raise val()
                if isinstance(val, BaseException):
                    raise val
                return val
            instance.poll.side_effect = side_effect

            with pytest.raises(ValueError, match="missing required field"):
                # Bug: the ValueError from the bad record propagates out
                # After fix, it should be caught and sent to DLQ, then processing continues
                run_consumer("test-group", ["test-topic"], handler)

        # With the bug: call_count is 2 (good + bad before crash), tx2 never processed
        # With the fix: call_count would be 3 (good + bad caught + good2 processed)
        assert call_count["n"] == 2
