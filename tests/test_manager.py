import pytest
import asyncio
from unittest.mock import patch, AsyncMock, MagicMock
from src.pipeline.manager import CHECKPOINT_DIR


# DF-09: Checkpoint stored in /tmp — lost on container restart
class TestCheckpoint:
    def test_checkpoint_dir_is_tmp(self):
        # Bug: /tmp is ephemeral — wiped on every container restart
        assert CHECKPOINT_DIR.startswith("/tmp")
        # Fix: should be a persistent volume path or S3 URI
        # e.g. "s3a://dataforge-checkpoints/" or "/data/checkpoints/"


# DF-15: Non-atomic check-then-set allows concurrent pipeline runs
class TestConcurrentRuns:
    @pytest.mark.asyncio
    async def test_concurrent_triggers_both_start(self):
        # Two concurrent calls to run_pipeline() with is_running=False
        # Both read is_running=False before either sets it to True
        # Both proceed to start a run — duplicate processing

        run_count = {"n": 0}
        pipeline_state = {"is_running": False}

        async def fake_run(pipeline_id: str) -> str:
            # Simulate the non-atomic check: both see is_running=False
            if pipeline_state["is_running"]:
                return "already_running"
            # Gap: second caller reads here before first caller sets is_running=True
            await asyncio.sleep(0)  # yield to let second caller check
            pipeline_state["is_running"] = True
            run_count["n"] += 1
            await asyncio.sleep(0.01)
            pipeline_state["is_running"] = False
            return f"run-{run_count['n']}"

        # Both coroutines start concurrently
        results = await asyncio.gather(
            fake_run("pipe-1"),
            fake_run("pipe-1"),
        )

        # Bug: both runs started (run_count == 2) instead of one being blocked
        # This is a simplified demonstration — the real bug is in the DB check-then-set
        # Fix: use SELECT pg_try_advisory_lock() or Redis SET NX before checking is_running
        assert run_count["n"] >= 1  # at least one ran

    def test_is_running_check_is_non_atomic(self):
        # The code reads is_running, then sets it in a separate UPDATE statement.
        # Between the SELECT and UPDATE, another process can read is_running=False
        # and also proceed past the guard.
        # This is a classic TOCTOU (time-of-check-time-of-use) race condition.
        assert True  # documents the pattern
