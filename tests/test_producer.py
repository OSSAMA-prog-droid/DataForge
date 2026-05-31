from unittest.mock import patch


# DF-06: acks='1' — silent data loss on leader failover
class TestProducerAcks:
    def test_producer_uses_acks_1_not_all(self):
        with patch("confluent_kafka.Producer") as MockProducer:
            from src.kafka.producer import get_producer
            get_producer()
            config = MockProducer.call_args[0][0]

            # Bug: acks='1' means only the partition leader acknowledges the write.
            # If the leader crashes before replicating to followers, the message is lost.
            assert config["acks"] == "1"

            # Fix: should be "all" (or -1) so all in-sync replicas must acknowledge.
            assert config.get("enable.idempotence") is None  # also missing

    def test_idempotence_not_enabled(self):
        with patch("confluent_kafka.Producer") as MockProducer:
            from src.kafka.producer import get_producer
            get_producer()
            config = MockProducer.call_args[0][0]
            # Without enable.idempotence=True, retries can produce duplicate messages
            assert "enable.idempotence" not in config
