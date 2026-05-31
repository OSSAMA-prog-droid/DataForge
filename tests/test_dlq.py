import pytest
from unittest.mock import patch


# DF-07: DLQ never consumed — failed records pile up forever
class TestDLQ:
    def test_drain_dlq_raises_not_implemented(self):
        from src.kafka.dlq import drain_dlq
        # The DLQ consumer is not implemented — calling it raises NotImplementedError
        with pytest.raises(NotImplementedError):
            drain_dlq()

    def test_send_to_dlq_writes_but_nobody_reads(self):
        # Verify that send_to_dlq produces to the topic but no consumer exists
        produced = []
        with patch("confluent_kafka.Producer") as MockProducer:
            instance = MockProducer.return_value
            instance.produce = lambda topic, value: produced.append((topic, value))
            instance.poll = lambda _: None

            from src.kafka.dlq import send_to_dlq
            send_to_dlq(
                record={"transaction_id": "tx_bad", "amount": None},
                error=ValueError("missing amount"),
                source_topic="dataforge.transactions",
            )

        # Record was written to DLQ topic — but no consumer will ever read it
        assert len(produced) == 1
        assert produced[0][0] == "dataforge.dlq"
