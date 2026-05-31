from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from src.spark.session import get_spark_session, get_jdbc_url, get_jdbc_properties
from src.utils.logging import get_logger

logger = get_logger(__name__)

# BUG DF-04: Full table scan on every pipeline run.
# read_transactions() loads ALL rows from the transactions table with no WHERE clause.
# The transactions table has 180M rows after 18 months of production data.
# Each pipeline run (every 15 minutes) reads 180M rows, processes ~50K new ones,
# and discards the rest. This takes 45 minutes and defeats the purpose of incremental
# processing. The watermark parameter is accepted but never used in the query.
# Fix: pass the watermark as a predicate: WHERE created_at > %(watermark)s

# BUG DF-12: Lookup table joined without broadcast hint.
# enrich_with_merchant() joins transactions (180M rows) with merchant_categories
# (800 rows) using a plain Spark join. Spark chooses a sort-merge join, shuffling
# all 180M transaction rows across the network. This takes ~45 minutes.
# With broadcast(), Spark sends the 800-row lookup to every executor in memory —
# the join completes in under 2 minutes with zero shuffle.
# Fix: wrap the small DataFrame in F.broadcast().


def read_transactions(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
    watermark: datetime,
) -> DataFrame:
    # BUG DF-04: watermark is received but never applied to the query
    return spark.read.jdbc(
        url=jdbc_url,
        table="transactions",
        # Missing: predicates=[f"created_at > '{watermark.isoformat()}'"]
        properties=jdbc_props,
    )


def read_merchant_categories(
    spark: SparkSession,
    jdbc_url: str,
    jdbc_props: dict,
) -> DataFrame:
    return spark.read.jdbc(
        url=jdbc_url,
        table="merchant_categories",
        properties=jdbc_props,
    )


def enrich_with_merchant(
    transactions: DataFrame,
    merchant_categories: DataFrame,
) -> DataFrame:
    # BUG DF-12: no broadcast() hint — forces a full shuffle join on 180M rows
    # Fix: merchant_categories = F.broadcast(merchant_categories)
    return transactions.join(
        merchant_categories,    # BUG DF-12: should be F.broadcast(merchant_categories)
        on="merchant_id",
        how="left",
    )


def deduplicate_transactions(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["transaction_id"])


def run_transactions_job(
    jdbc_url: str,
    jdbc_props: dict,
    watermark: datetime,
    output_table: str = "warehouse.transactions",
) -> int:
    spark = get_spark_session("DataForge-Transactions")

    logger.info("transactions_job_started", watermark=watermark.isoformat())

    transactions = read_transactions(spark, jdbc_url, jdbc_props, watermark)
    categories = read_merchant_categories(spark, jdbc_url, jdbc_props)
    enriched = enrich_with_merchant(transactions, categories)
    deduped = deduplicate_transactions(enriched)

    count = deduped.count()

    deduped.write.jdbc(
        url=jdbc_url,
        table=output_table,
        mode="append",
        properties=jdbc_props,
    )

    logger.info("transactions_job_finished", records_written=count)
    return count
