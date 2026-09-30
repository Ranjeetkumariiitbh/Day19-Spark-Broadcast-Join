import contextlib
import io
import time

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def broadcast_join(txns: DataFrame, branches: DataFrame) -> DataFrame:
    """Small table is shipped once to every executor; big table is NOT shuffled."""
    return txns.join(F.broadcast(branches), on="branch_id", how="inner")


def sort_merge_join(txns: DataFrame, branches: DataFrame) -> DataFrame:
    """Force a shuffle sort-merge join with the 'merge' hint (hints win over the
    broadcast threshold, so this is deterministic)."""
    return txns.join(branches.hint("merge"), on="branch_id", how="inner")


def branch_revenue_report(joined: DataFrame) -> DataFrame:
    return (
        joined.groupBy("region", "branch_name")
        .agg(
            F.count("*").alias("txn_count"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
        )
        .orderBy(F.desc("total_amount"))
    )


def capture_plan(df: DataFrame, mode: str = "formatted") -> str:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        df.explain(mode=mode)
    return buf.getvalue()


def time_action(df: DataFrame, runs: int = 3) -> float:
    """Run a full action (noop sink = execute everything, write nothing) and return
    the best wall-clock time in seconds. Best-of-N reduces JVM warm-up noise."""
    best = float("inf")
    for _ in range(runs):
        start = time.perf_counter()
        df.write.format("noop").mode("overwrite").save()
        best = min(best, time.perf_counter() - start)
    return best
