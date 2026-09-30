# Day 19 - Spark Broadcast Join (PySpark)

Joins a **large transaction fact table** with a **small branch reference table** and compares
a **Broadcast Hash Join** with a **Shuffle Sort-Merge Join**: same result, very different execution.

## Project structure

```
Day19-Spark-Broadcast-Join/
├── README.md
├── requirements.txt
├── .gitignore
├── push_to_github.sh
├── data/branches.csv            # small reference table (20 branches)
├── src/
│   ├── spark_session.py         # local session, AQE off so plans are deterministic
│   ├── data_generator.py        # N-row transactions (spark.range) + branches loader
│   ├── join_strategies.py       # broadcast join, sort-merge join, report, plan capture, timing
│   └── main.py                  # runs the whole demo
├── docs/plans/                  # physical plans written by main.py (commit them as evidence)
└── output/                      # generated report (ignored by git)
```

## Run

```bash
python -m venv .venv && source .venv/bin/activate     # Windows: .venv\Scripts\activate
pip install -r requirements.txt                        # needs Java 17 or 21
python -m src.main                                     # 5M transactions
python -m src.main --rows 20000000 --runs 3            # bigger = clearer difference
```

While it runs, open http://localhost:4040 (Spark UI) and compare the SQL / Stages tabs.

## The two join strategies

```python
from pyspark.sql import functions as F

# Broadcast hash join
txns.join(F.broadcast(branches), "branch_id")

# Shuffle sort-merge join (forced with a hint)
txns.join(branches.hint("merge"), "branch_id")
```

| | Broadcast Hash Join | Shuffle Sort-Merge Join |
|---|---|---|
| Large table moved? | **No** - stays where it is | **Yes** - hash-partitioned by join key across the network |
| Small table | Collected to driver, sent once to every executor, held as an in-memory hash table | Also shuffled and sorted |
| Sorting | None | Both sides sorted per partition |
| Stages | Join happens inside the scan stage (no extra stage) | Extra shuffle stage boundary |
| Plan nodes | `BroadcastExchange` + `BroadcastHashJoin` | `Exchange hashpartitioning` + `Sort` (x2) + `SortMergeJoin` |
| Skew risk | None on the big side | Hot keys overload one partition (here ~1/20 of rows per branch, fine; real data may not be) |
| Limit | Small side must fit in driver + executor memory | Scales to any size |

### Why broadcast wins for millions of transactions x a few branches
- Shuffling millions of rows means serializing, writing shuffle files to disk, network transfer and reading them back - all to match against 20 rows.
- Broadcasting 20 rows costs almost nothing, and every transaction partition joins locally with a hash lookup.
- With only 20 distinct `branch_id` values, a sort-merge join also produces at most 20 busy reducer partitions out of `spark.sql.shuffle.partitions`, so most of the parallelism is wasted.

## Transformations vs actions

| Transformations (lazy, build the plan) | Actions (trigger a job) |
|---|---|
| `spark.range`, `withColumn`, `join`, `broadcast`, `hint`, `groupBy().agg`, `orderBy` | `show`, `count`, `write` (noop / csv), `exceptAll(...).count()` |

`join` and `groupBy` are *wide* transformations (need data from many partitions); `withColumn`/`filter` are *narrow*.
`explain()` does not run the query - it only prints the plan Catalyst produced.

## Partitions and shuffle behaviour

- Transactions are generated in **16 partitions** (`numPartitions` in `spark.range`), so 16 tasks scan in parallel.
- **Broadcast join**: no shuffle for the join itself; partition count of the large side is preserved.
- **Sort-merge join**: `Exchange hashpartitioning(branch_id, 8)` repartitions into `spark.sql.shuffle.partitions` (8 here). Check *Shuffle Write/Read* in the Stages tab.
- The report's `groupBy` + `orderBy` adds its own (much smaller, pre-aggregated) shuffles in **both** versions.
- AQE is disabled on purpose. With AQE on (default since Spark 3.2) Spark can turn a sort-merge join into a broadcast join at runtime once it sees the small side's real size.

## Reading the execution plan

```
Broadcast:   HashAggregate <- BroadcastHashJoin <- (Project, Range) + BroadcastExchange <- FileScan csv
Sort-merge:  HashAggregate <- SortMergeJoin <- Sort <- Exchange hashpartitioning <- Range
                                             <- Sort <- Exchange hashpartitioning <- FileScan csv
```
Plans are saved by `main.py` to `docs/plans/`.

## Performance considerations

1. **Automatic threshold**: Spark broadcasts a side automatically if its estimated size is below `spark.sql.autoBroadcastJoinThreshold` (default 10 MB). Use `F.broadcast()` when stats are missing or wrong.
2. **Memory**: the broadcast table lives on the driver and on every executor. Too large = OOM or slow broadcast. Keep it to tens or low hundreds of MB.
3. **Join type matters**: you can broadcast the right side of inner/left joins and the left side of right joins, but not the preserved side of an outer join.
4. **Stale data**: the broadcast is a snapshot taken when the query runs.
5. **Timeout**: `spark.sql.broadcastTimeout` (default 300 s) fails the query if the broadcast takes too long.
6. **Local mode caveat**: there is no real network here, so the measured gap understates what you see on a cluster.
7. **Reuse**: join on an integer key and select only the columns you need from the dimension before broadcasting.

## Push to GitHub

```bash
chmod +x push_to_github.sh
./push_to_github.sh https://github.com/<your-username>/Day19-Spark-Broadcast-Join.git
```
