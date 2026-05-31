import pytest


# DF-14: AVG with null rows returns NaN for entire window
class TestAggregations:
    def test_avg_with_null_amount_returns_nan(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from src.spark.jobs.aggregations import calculate_merchant_metrics

        spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        data = [
            ("merch-1", "electronics", "cust-1", 100.0),
            ("merch-1", "electronics", "cust-2", 200.0),
            ("merch-1", "electronics", "cust-3", None),  # NULL amount
            ("merch-2", "clothing",    "cust-4", 50.0),
        ]
        df = spark.createDataFrame(data, ["merchant_id", "category", "customer_id", "amount"])

        result = calculate_merchant_metrics(df)
        rows = {r["merchant_id"]: r for r in result.collect()}

        import math
        # Bug: one NULL in merch-1 makes avg_transaction_value NaN for the whole merchant
        assert math.isnan(rows["merch-1"]["avg_transaction_value"])

        # merch-2 (no NULLs) is fine
        assert rows["merch-2"]["avg_transaction_value"] == 50.0

        # Fix: filter(F.col("amount").isNotNull()) before groupBy
        # After fix: merch-1 avg should be (100+200)/2 = 150.0

        spark.stop()

    def test_hourly_totals_nan_propagation(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from src.spark.jobs.aggregations import calculate_hourly_totals
        from datetime import datetime

        spark = SparkSession.builder.master("local[1]").appName("test2").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        data = [
            ("merch-1", "tx-1", 100.0, datetime(2026, 5, 31, 10, 0, 0)),
            ("merch-1", "tx-2", None,  datetime(2026, 5, 31, 10, 15, 0)),  # NULL
        ]
        df = spark.createDataFrame(data, ["merchant_id", "transaction_id", "amount", "created_at"])

        result = calculate_hourly_totals(df)
        row = result.collect()[0]

        import math
        assert math.isnan(row["avg_amount"])  # demonstrates the bug

        spark.stop()
