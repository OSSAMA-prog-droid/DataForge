from pyspark.sql import SparkSession
from src.config import settings

# BUG DF-08: SparkSession created with no memory configuration.
# Default executor memory is 512MB and driver memory is 1GB.
# DataForge processes partitions of ~2M rows at peak (end-of-day batch).
# A 2M-row partition of transaction records averages ~800 bytes/row = ~1.6GB.
# With 512MB executor memory, Spark spills to disk → 10× slower → job timeout.
# On datasets > 3M rows, the executor OOMs and the job fails with no useful error.
# Fix: configure executor.memory=4g, driver.memory=2g, and enable dynamic allocation.


def get_spark_session(app_name: str = "DataForge") -> SparkSession:
    # BUG DF-08: no .config() calls for memory, cores, or shuffle partitions
    return (
        SparkSession.builder
        .appName(app_name)
        # Missing: .config("spark.executor.memory", "4g")
        # Missing: .config("spark.driver.memory", "2g")
        # Missing: .config("spark.sql.shuffle.partitions", "200")
        # Missing: .config("spark.dynamicAllocation.enabled", "true")
        .getOrCreate()
    )


def get_jdbc_url(host: str, port: int, db: str) -> str:
    return f"jdbc:postgresql://{host}:{port}/{db}"


def get_jdbc_properties(user: str, password: str) -> dict:
    return {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }
