from unittest.mock import patch, MagicMock


# DF-10: No connection pool limit
class TestPostgresSink:
    def test_pool_maxconn_is_too_high(self):
        with patch("psycopg2.pool.ThreadedConnectionPool") as MockPool:
            # Reset module state
            import src.connectors.postgres_sink as sink_module
            sink_module._pool = None

            from src.connectors.postgres_sink import _get_pool
            _get_pool()

            call_kwargs = MockPool.call_args

            # Bug: maxconn=50 per worker — with 8 workers this is 400 connections
            # PostgreSQL default max_connections is 100
            maxconn = call_kwargs[1].get("maxconn") or call_kwargs[0][1]
            assert maxconn == 50  # demonstrates the bug

            # Fix: maxconn should be <= 10 per worker
            # Total connections = workers × maxconn must stay under DB limit

            sink_module._pool = None  # reset for other tests
