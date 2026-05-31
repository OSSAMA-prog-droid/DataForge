import pytest


# DF-08: SparkSession created with no memory configuration
class TestSparkSession:
    def test_spark_session_has_no_memory_config(self):
        pytest.importorskip("pyspark")
        from pyspark.sql import SparkSession
        from src.spark.session import get_spark_session

        spark = get_spark_session("test-no-memory")
        conf = spark.sparkContext.getConf()

        # Bug: executor memory defaults to 512m — OOM on partitions > 1M rows
        executor_mem = conf.get("spark.executor.memory", "512m")
        assert executor_mem in ("512m", "1g", None, "512m")  # default or unset

        # Fix: should be set to at least "4g" for DataForge's partition sizes
        # assert conf.get("spark.executor.memory") == "4g"

        spark.stop()
