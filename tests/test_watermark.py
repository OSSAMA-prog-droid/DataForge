import pytest
import fakeredis
from unittest.mock import patch
from datetime import datetime, timezone


# DF-11: datetime.now() — timezone-naive watermark
class TestWatermark:
    def test_get_watermark_uses_datetime_now_not_utcnow(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch("src.pipeline.watermark.get_redis", return_value=r):
            from src.pipeline.watermark import get_watermark, advance_watermark

            # advance_watermark uses datetime.now() — timezone-naive
            wm = advance_watermark("pipeline-1")

            # Bug: returned datetime has no tzinfo — it's local server time
            assert wm.tzinfo is None  # demonstrates the bug

            # Fix: wm.tzinfo should be timezone.utc

    def test_advance_watermark_is_timezone_naive(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch("src.pipeline.watermark.get_redis", return_value=r):
            from src.pipeline.watermark import advance_watermark

            wm = advance_watermark("pipeline-2")
            # Bug: no timezone info — on UTC+5 server, this is 5 hours ahead of UTC
            # Hourly aggregation windows will be wrong for tenants in other timezones
            assert wm.tzinfo is None

    def test_watermark_stored_without_timezone(self):
        r = fakeredis.FakeRedis(decode_responses=True)
        with patch("src.pipeline.watermark.get_redis", return_value=r):
            from src.pipeline.watermark import advance_watermark, WATERMARK_KEY_PREFIX

            advance_watermark("pipeline-3")
            stored = r.get(f"{WATERMARK_KEY_PREFIX}pipeline-3")
            # Bug: ISO format string has no timezone offset (e.g. "2026-05-31T14:30:00")
            # Fix: should end with "+00:00" or "Z"
            assert stored is not None
            assert "+" not in stored and stored.endswith(("0", "1", "2", "3", "4", "5", "6", "7", "8", "9"))
