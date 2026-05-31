import pytest
import fakeredis
from unittest.mock import patch


# DF-05: Redis dedup TTL shorter than Kafka retry window
class TestDedup:
    def test_dedup_ttl_is_60_seconds(self):
        from src.pipeline.dedup import DEDUP_TTL_SECONDS
        # Bug: 60 seconds is shorter than Kafka's default retry window of 5 minutes
        assert DEDUP_TTL_SECONDS == 60
        # Fix: should be >= 600 seconds

    def test_duplicate_detected_within_ttl(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch("src.pipeline.dedup.get_shared_redis", return_value=r):
            from src.pipeline.dedup import is_duplicate, mark_processed
            mark_processed("event-123")
            assert is_duplicate("event-123") is True

    def test_duplicate_missed_after_ttl_expires(self):
        # Scenario: event published, dedup key set with 60s TTL.
        # Kafka retries after 90 seconds (within Kafka's retry window).
        # The dedup key has expired — is_duplicate returns False.
        # The event is processed a second time.
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch("src.pipeline.dedup.get_shared_redis", return_value=r):
            from src.pipeline.dedup import is_duplicate, DEDUP_KEY_PREFIX

            event_id = "event-456"
            # Simulate: key was set and has now expired (not present in Redis)
            assert r.get(f"{DEDUP_KEY_PREFIX}{event_id}") is None
            # is_duplicate returns False — event processed again (the bug)
            assert is_duplicate(event_id) is False
