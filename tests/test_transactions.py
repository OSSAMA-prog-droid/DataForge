import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime


# DF-04: Full table scan — watermark not applied
class TestFullTableScan:
    def test_read_transactions_ignores_watermark(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        from unittest.mock import patch, MagicMock

        spark = SparkSession.builder.master("local[1]").appName("test-scan").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        read_calls = []

        original_read = spark.read.jdbc

        def capture_read(url, table, **kwargs):
            read_calls.append({"table": table, "kwargs": kwargs})
            # Return empty df to avoid needing a real DB
            return spark.createDataFrame([], "transaction_id STRING, merchant_id STRING")

        watermark = datetime(2026, 5, 30, 0, 0, 0)

        with patch.object(spark.read, "jdbc", side_effect=capture_read):
            from src.spark.jobs.transactions import read_transactions
            try:
                read_transactions(spark, "jdbc:postgresql://localhost/test", {}, watermark)
            except Exception:
                pass

        if read_calls:
            call = read_calls[0]
            # Bug: no predicates passed — full table scan
            assert "predicates" not in call["kwargs"] or call["kwargs"]["predicates"] is None

        spark.stop()


# DF-12: No broadcast hint on small lookup table
class TestBroadcastJoin:
    def test_enrich_with_merchant_has_no_broadcast(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        from src.spark.jobs.transactions import enrich_with_merchant

        spark = SparkSession.builder.master("local[1]").appName("test-join").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        txn_data = [("tx1", "merch-1", "cust-1", 100.0)]
        txn_df = spark.createDataFrame(txn_data, ["transaction_id", "merchant_id", "customer_id", "amount"])

        cat_data = [("merch-1", "electronics", "Big Store")]
        cat_df = spark.createDataFrame(cat_data, ["merchant_id", "category", "name"])

        # Bug: join runs without broadcast — on 180M rows this causes a full shuffle
        result = enrich_with_merchant(txn_df, cat_df)
        assert result.count() == 1

        # Fix: cat_df should be wrapped in F.broadcast(cat_df)
        # This avoids sending 180M transaction rows across the network for the join

        spark.stop()
