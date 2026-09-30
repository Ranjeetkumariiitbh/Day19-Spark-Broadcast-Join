from pyspark.sql import SparkSession


def get_spark(app_name: str = "Day19-Broadcast-Join", shuffle_partitions: int = 8) -> SparkSession:
    """Local SparkSession.

    AQE is disabled on purpose so that the physical plans shown in this demo are
    exactly what the planner chose (with AQE on, Spark may silently convert a
    sort-merge join into a broadcast join at runtime, hiding the comparison).
    """
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
