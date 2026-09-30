import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

NUM_BRANCHES = 20


def create_transactions(spark: SparkSession, num_rows: int, num_partitions: int = 16) -> DataFrame:
    """Large fact table. spark.range() generates rows in parallel, no input files needed."""
    return (
        spark.range(1, num_rows + 1, step=1, numPartitions=num_partitions)
        .withColumnRenamed("id", "txn_id")
        .withColumn("branch_id", (F.rand(seed=1) * NUM_BRANCHES + 1).cast("int"))
        .withColumn("customer_id", (F.rand(seed=2) * 500_000 + 1).cast("int"))
        .withColumn("amount", F.round(F.rand(seed=3) * 5000 + 10, 2))
        .withColumn(
            "txn_type",
            F.when(F.rand(seed=4) < 0.5, "DEBIT")
            .when(F.rand(seed=5) < 0.8, "CREDIT")
            .otherwise("TRANSFER"),
        )
        .withColumn("txn_date", F.expr("date_add(DATE'2025-01-01', cast(rand(6) * 365 as int))"))
    )


def create_branches(spark: SparkSession) -> DataFrame:
    """Small dimension table (20 rows) read from CSV."""
    path = os.path.join(os.path.dirname(__file__), "..", "data", "branches.csv")
    return spark.read.option("header", True).option("inferSchema", True).csv(os.path.abspath(path))
