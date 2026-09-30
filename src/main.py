"""Day 19 - Broadcast Join vs Shuffle Sort-Merge Join (PySpark).

Usage:
    python -m src.main                 # 5 million transactions
    python -m src.main --rows 20000000
"""
import argparse
import os

from src.data_generator import create_branches, create_transactions
from src.join_strategies import (
    branch_revenue_report,
    broadcast_join,
    capture_plan,
    sort_merge_join,
    time_action,
)
from src.spark_session import get_spark

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def banner(title: str) -> None:
    print("\n" + "=" * 78 + f"\n{title}\n" + "=" * 78)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--runs", type=int, default=3, help="timing runs per strategy")
    args = parser.parse_args()

    spark = get_spark()

    # ---- 1. Data (transformations are lazy, nothing runs yet) ---------------
    txns = create_transactions(spark, args.rows)
    branches = create_branches(spark)

    banner("1. DATASETS")
    print(f"transactions partitions : {txns.rdd.getNumPartitions()}")
    print(f"branches partitions     : {branches.rdd.getNumPartitions()}")
    txns.printSchema()
    txns.show(5)                                            # ACTION
    branches.show(5)                                        # ACTION
    print(f"transaction rows        : {txns.count():,}")    # ACTION
    print(f"branch rows             : {branches.count()}")  # ACTION

    # ---- 2. Broadcast join --------------------------------------------------
    bhj = broadcast_join(txns, branches)
    banner("2. BROADCAST JOIN - physical plan")
    bhj_plan = capture_plan(bhj)
    print(bhj_plan)

    # ---- 3. Sort-merge join -------------------------------------------------
    smj = sort_merge_join(txns, branches)
    banner("3. SHUFFLE SORT-MERGE JOIN - physical plan")
    smj_plan = capture_plan(smj)
    print(smj_plan)

    os.makedirs(os.path.join(ROOT, "docs", "plans"), exist_ok=True)
    for name, plan in (("broadcast_join", bhj_plan), ("sort_merge_join", smj_plan)):
        with open(os.path.join(ROOT, "docs", "plans", f"{name}_plan.txt"), "w") as f:
            f.write(plan)

    # ---- 4. Same answer? ----------------------------------------------------
    banner("4. CORRECTNESS CHECK + BUSINESS REPORT")
    bhj_report = branch_revenue_report(bhj)
    smj_report = branch_revenue_report(smj)
    assert bhj_report.exceptAll(smj_report).count() == 0, "join results differ!"
    assert smj_report.exceptAll(bhj_report).count() == 0, "join results differ!"
    print("Both strategies return identical results.")
    bhj_report.show(10, truncate=False)                     # ACTION

    # ---- 5. Timing ----------------------------------------------------------
    banner("5. PERFORMANCE COMPARISON")
    print("post-shuffle partitions (spark.sql.shuffle.partitions) = "
          f"{spark.conf.get('spark.sql.shuffle.partitions')}")
    t_bhj = time_action(bhj_report, args.runs)
    t_smj = time_action(smj_report, args.runs)
    print(f"Broadcast hash join : {t_bhj:6.2f} s")
    print(f"Sort-merge join     : {t_smj:6.2f} s")
    print(f"Ratio (SMJ / BHJ)   : {t_smj / t_bhj:4.2f}x  (local mode; on a cluster the "
          "shuffle also crosses the network, so the gap is usually larger)")

    # ---- 6. Save result -----------------------------------------------------
    bhj_report.coalesce(1).write.mode("overwrite").option("header", True).csv(
        os.path.join(ROOT, "output", "branch_revenue")
    )
    print("\nReport written to output/branch_revenue/")

    banner("7. WHAT TO LOOK FOR IN THE PLANS")
    print("Broadcast  : BroadcastExchange -> BroadcastHashJoin. No Exchange/Sort on transactions.")
    print("Sort-merge : Exchange hashpartitioning(branch_id) + Sort on BOTH sides -> SortMergeJoin.")
    print("Open http://localhost:4040 while running to compare stages and shuffle read/write.")
    spark.stop()


if __name__ == "__main__":
    main()
