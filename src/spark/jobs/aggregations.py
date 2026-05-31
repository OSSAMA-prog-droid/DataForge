from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from src.spark.session import get_spark_session
from src.utils.logging import get_logger

logger = get_logger(__name__)

# BUG DF-14: AVG() aggregation with no null filter.
# calculate_merchant_metrics() computes average transaction value per merchant.
# If any transaction has a NULL amount (e.g. from a failed payment capture where
# amount was not populated), F.avg("amount") propagates NULL/NaN for the entire
# merchant's window. The NaN is written to the warehouse metrics table.
# Downstream dashboards show "NaN" for that merchant's revenue for the entire hour.
# Fix: filter out nulls before aggregating: df.filter(F.col("amount").isNotNull())
# or use F.avg(F.when(F.col("amount").isNotNull(), F.col("amount"))).


def calculate_hourly_totals(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("hour", F.date_trunc("hour", F.col("created_at")))
        .groupBy("hour", "merchant_id")
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("transaction_id").alias("transaction_count"),
            # BUG DF-14: avg propagates NULL if any row has NULL amount
            F.avg("amount").alias("avg_amount"),   # should filter nulls first
            F.max("amount").alias("max_amount"),
            F.min("amount").alias("min_amount"),
        )
    )


def calculate_merchant_metrics(df: DataFrame) -> DataFrame:
    # BUG DF-14: no .filter(F.col("amount").isNotNull()) before aggregating
    return (
        df
        .groupBy("merchant_id", "category")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.avg("amount").alias("avg_transaction_value"),  # BUG DF-14
            F.countDistinct("customer_id").alias("unique_customers"),
            F.count("*").alias("total_transactions"),
        )
    )


def calculate_daily_summary(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("date", F.to_date(F.col("created_at")))
        .groupBy("date")
        .agg(
            F.sum("amount").alias("daily_total"),
            F.avg("amount").alias("daily_avg"),  # BUG DF-14: same issue
            F.count("*").alias("daily_count"),
        )
    )


def run_aggregations_job(
    jdbc_url: str,
    jdbc_props: dict,
    source_table: str = "warehouse.transactions",
) -> None:
    spark = get_spark_session("DataForge-Aggregations")

    df = spark.read.jdbc(url=jdbc_url, table=source_table, properties=jdbc_props)

    hourly = calculate_hourly_totals(df)
    hourly.write.jdbc(
        url=jdbc_url,
        table="warehouse.hourly_totals",
        mode="overwrite",
        properties=jdbc_props,
    )

    metrics = calculate_merchant_metrics(df)
    metrics.write.jdbc(
        url=jdbc_url,
        table="warehouse.merchant_metrics",
        mode="overwrite",
        properties=jdbc_props,
    )

    logger.info("aggregations_job_finished")
