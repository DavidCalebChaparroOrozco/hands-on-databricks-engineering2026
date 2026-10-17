# Spark Joins
How Spark physically executes a join: move the small table, or move both
- Broadcast Hash Join
- Sort-Merge Join
- Shuffle Hash Join
- Join Hints

---

## Join Type vs. Join Strategy

These are two different questions:

| Question                              | Answer                                      | Where it is covered                                                                                              |
| ------------------------------------- | ------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Which rows should the result contain? | Join type: INNER, LEFT, RIGHT, FULL, ANTI   | [Join Operations](../../../part03_spark_sql/ch04_spark_sql/notes/UnionIntersectMinus&PivotinSparkSQL.md)         |
| How does Spark compute that result?   | Join strategy: broadcast, sort-merge, hash  | This file                                                                                                        |

The same `INNER JOIN` can run with different strategies, and the result is identical. What changes is the cost: how much data moves across the network, how much memory is needed, and whether the data is sorted.

Key takeaway: The join type decides the answer. The join strategy decides how expensive it is to get that answer.

---

## The Core Problem

To join two datasets on a key, every row with key `42` from the left side must meet every row with key `42` from the right side, on the same executor.

But the data is spread across many partitions on many executors:

```text
Executor 1: left rows (keys 1, 42, 7)    right rows (keys 42, 9)
Executor 2: left rows (keys 42, 3, 8)    right rows (keys 7, 42)
Executor 3: left rows (keys 9, 42, 1)    right rows (keys 3, 8)
```

Spark has two ways to make matching keys meet:

1. Send the small side to every executor. The large side never moves. This is the broadcast approach.
2. Move both sides so that equal keys land in the same partition. This is the shuffle approach, and it is a wide transformation.

> Joins are wide transformations unless Spark can avoid shuffling the large side. See `NarrowVSWide&ShuffleStages.md`.

---

## Broadcast Hash Join

Spark sends a complete copy of the small side to every executor. Each executor builds an in-memory hash table from it and processes its own partitions of the large side against that table.

```text
Small table (zones)
        │
        │  broadcast: a full copy goes to every executor
        ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│  Executor 1   │ │  Executor 2   │ │  Executor 3   │
│  zones (copy) │ │  zones (copy) │ │  zones (copy) │
│  trips part 1 │ │  trips part 2 │ │  trips part 3 │
└───────────────┘ └───────────────┘ └───────────────┘
   each task looks up its trips in the local hash table
```

The large side (`trips`) is not shuffled. Each task reads its own partition and looks up each key in the local hash table, so no data from the large side crosses the network.

### Example

```python
trips = spark.read.parquet("/Volumes/main/taxi/raw/trips")      # millions of rows
zones = spark.read.csv("/Volumes/main/taxi/raw/zones.csv",      # a few hundred rows
                       header=True, inferSchema=True)

result = trips.join(zones, trips.PULocationID == zones.LocationID)
result.explain()
```

A simplified plan (shape verified on Spark 4.2.0, local mode):

```text
BroadcastHashJoin [PULocationID], [LocationID], Inner, BuildRight
:- (trips: scan only, no Exchange)
+- BroadcastExchange HashedRelationBroadcastMode
   +- (zones: scan)
```

`BuildRight` means the right side (`zones`) is the one that was broadcast and turned into the hash table. There is no `Exchange hashpartitioning` on the large side, which means no shuffle for `trips`.

### When Spark chooses it automatically

Spark broadcasts a side when its estimated size is below `spark.sql.autoBroadcastJoinThreshold`.

- Default: `10485760` bytes (10 MB). Confirmed in the Apache Spark documentation and on a Spark 4.2.0 session.
- Setting it to `-1` disables automatic broadcasting.
- The comparison uses Spark's size estimate (statistics), not the actual row count at runtime.

### Strengths and limits

- Strength: the large side is never shuffled, so there is no shuffle cost and no skew caused by the join key on that side.
- Limit: the small side must fit in memory on every executor, and it is first gathered and then distributed, so the driver is involved too.
- Limit: broadcasting something that is not really small can cause memory problems instead of saving time.
- Limit: it needs an equi-join key to use a hash table. Without one, Spark falls back to a slower broadcast nested loop join (see below).

Key takeaway: Broadcast the small side so the big side never has to move.

---

## Sort-Merge Join

When both sides are large, broadcasting is not an option. Spark shuffles both sides by the join key, sorts each partition, and then merges the two sorted streams.

```text
Left table                            Right table
    │                                      │
    ▼                                      ▼
Exchange hashpartitioning(k, 200)    Exchange hashpartitioning(k, 200)
    │                                      │
    ▼                                      ▼
Sort by k (inside each partition)    Sort by k (inside each partition)
    │                                      │
    └───────────────► merge ◄──────────────┘
                        │
                        ▼
                 SortMergeJoin
```

1. Shuffle: both sides are hash-partitioned by the join key into the same number of partitions, so rows with equal keys end up in partitions with the same number.
2. Sort: inside each partition, both sides are sorted by the key.
3. Merge: because both sides are sorted, Spark walks through them together, matching keys as it goes, without needing to hold an entire side in memory as a hash table.

### Example (broadcast disabled to force it)

```python
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

result = trips.join(other_big_table, "trip_id")
result.explain()
```

A simplified plan (shape verified on Spark 4.2.0, local mode):

```text
SortMergeJoin [trip_id], [trip_id], Inner
:- Sort [trip_id ASC NULLS FIRST]
:  +- Exchange hashpartitioning(trip_id, 200)
+- Sort [trip_id ASC NULLS FIRST]
   +- Exchange hashpartitioning(trip_id, 200)
```

The number `200` is `spark.sql.shuffle.partitions`, whose default is 200 on the session tested.

### Strengths and limits

- Strength: it scales to two very large datasets, because sorted partitions can be merged as streams and the sort can use disk if memory is short.
- Cost: two shuffles (one per side) plus two sorts. This is usually the most expensive strategy in terms of network and CPU.
- Risk: a hot key sends a huge number of rows to one partition. See `DataSkew.md`.
- Risk: sort buffers can run out of memory and spill to disk. See `DataSpill.md`.
- Default preference: `spark.sql.join.preferSortMergeJoin` is `true` on the session tested, so Spark prefers this strategy over the shuffle hash join when it cannot broadcast.

Key takeaway: Sort-merge is the general-purpose strategy for large-to-large joins. It is robust, but it pays for shuffling and sorting both sides.

---

## Shuffle Hash Join

Like sort-merge, both sides are shuffled by the join key. The difference is what happens inside each partition: Spark skips the sort and instead builds a hash table from the smaller side of that partition and probes it with the other side.

```text
Left table                            Right table
    │                                      │
    ▼                                      ▼
Exchange hashpartitioning(k, 200)    Exchange hashpartitioning(k, 200)
    │                                      │
    ▼                                      ▼
  (no sort)                           build hash table per partition
    │                                      │
    └────────── probe the hash table ◄─────┘
                        │
                        ▼
                ShuffledHashJoin
```

A simplified plan (verified with `SHUFFLE_HASH` hint, Spark 4.2.0, local mode):

```text
ShuffledHashJoin [k], [k], Inner, BuildRight
:- Exchange hashpartitioning(k, 200)
+- Exchange hashpartitioning(k, 200)
```

Notice there is no `Sort` node, which is the difference from the sort-merge plan.

### When it makes sense

- One side is much smaller than the other, but still too large to broadcast.
- Avoiding the sort is worth more than the memory needed for the per-partition hash table.

### Strengths and limits

- Strength: no sort, so less CPU work than sort-merge.
- Risk: the build side of each partition has to fit in the task's execution memory. A skewed partition makes this worse.
- In practice, Spark does not pick it by default when sort-merge is preferred. It is most often used by asking for it with a hint.

Key takeaway: Shuffle hash join trades the sort for a hash table. It helps when one side is clearly smaller, and it hurts when that hash table does not fit in memory.

---

## Side-by-Side Comparison

| Aspect                  | Broadcast Hash Join                 | Sort-Merge Join                          | Shuffle Hash Join                          |
| ----------------------- | ----------------------------------- | ---------------------------------------- | ------------------------------------------ |
| Shuffles the large side | No                                  | Yes                                      | Yes                                        |
| Shuffles the small side | No (it is broadcast instead)        | Yes                                      | Yes                                        |
| Sorts the data          | No                                  | Yes, both sides                          | No                                         |
| Memory pattern          | Full small side on every executor   | Sort buffers (can spill)                 | Hash table of one side per partition       |
| Best when               | One side is small                   | Both sides are large                     | One side is much smaller, but not tiny     |
| Main risk               | Broadcasting something too big      | Shuffle and sort cost, skew, spill       | Hash table too big for memory              |
| Plan node               | `BroadcastHashJoin`                 | `SortMergeJoin`                          | `ShuffledHashJoin`                         |

### Joins without an equality condition

Hash-based strategies need an equality on the key. For a condition such as `a.k < b.k`, Spark falls back to:

```text
BroadcastNestedLoopJoin BuildRight, Inner, (k < k)
+- BroadcastExchange IdentityBroadcastMode
```

(shape verified on Spark 4.2.0). It compares rows pair by pair, so it is much more expensive. If neither side can be broadcast, a non-equi join becomes a `CartesianProduct` with the condition applied afterwards (also verified), and a cross join without any condition is a `CartesianProduct` as well. Watch for accidental non-equi joins in large workloads.

---

## How Spark Chooses (Simplified)

```text
Is there a join hint?
   Yes → try the hinted strategy (it is not guaranteed)
   No  ↓
Is it an equi-join and one side is estimated below the broadcast threshold?
   Yes → Broadcast Hash Join
   No  ↓
Is it an equi-join with sortable keys?
   Yes → Sort-Merge Join (preferred by default)
   No  ↓
No equality condition
       → Broadcast Nested Loop Join or Cartesian Product
```

This is a simplified model built from the Spark documentation and from plans observed in experiments, not Spark's full planner logic. The real planner has more rules, and they change between versions.

---

## Join Hints

Hints tell Spark which strategy you want, overriding its size estimates.

| Hint                   | Strategy requested                                 |
| ---------------------- | -------------------------------------------------- |
| `BROADCAST`            | Broadcast hash join (or broadcast nested loop join) |
| `MERGE`                | Sort-merge join                                    |
| `SHUFFLE_HASH`         | Shuffle hash join                                  |
| `SHUFFLE_REPLICATE_NL` | Shuffle-and-replicate nested loop join             |

### SQL

```sql
SELECT /*+ BROADCAST(z) */ *
FROM trips t
JOIN zones z ON t.PULocationID = z.LocationID;
```

### DataFrame API

```python
from pyspark.sql.functions import broadcast

trips.join(broadcast(zones), trips.PULocationID == zones.LocationID)

trips.join(zones.hint("broadcast"), trips.PULocationID == zones.LocationID)
trips.join(zones.hint("shuffle_hash"), trips.PULocationID == zones.LocationID)
trips.join(zones.hint("merge"), trips.PULocationID == zones.LocationID)
```

### Priority when hints conflict

Per the Apache Spark documentation: `BROADCAST` over `MERGE` over `SHUFFLE_HASH` over `SHUFFLE_REPLICATE_NL`.

### Correction: a hint is a request, not a command

The documentation states there is no guarantee that Spark will use the hinted strategy, because a given strategy may not support every join type.

A concrete case, tested on Spark 4.2.0 with automatic broadcasting disabled:

```python
# LEFT OUTER join, BROADCAST hint on the left (preserved) side
small.hint("broadcast").join(big, "k", "left")
# → SortMergeJoin [k], [k], LeftOuter   (hint ignored)

# Same join, hint on the right (non-preserved) side
big.join(small.hint("broadcast"), "k", "left")
# → BroadcastHashJoin ... LeftOuter, BuildRight   (hint honored)

# INNER join: either side can be broadcast
small.hint("broadcast").join(big, "k")
# → BroadcastHashJoin ... Inner, BuildLeft
```

In a LEFT OUTER join, the preserved side (the left) cannot be the broadcast side, so a hint on it has no effect.

Key takeaway: Always confirm with `explain()` that the hint did what you expected.

---

## AQE Can Change the Strategy at Runtime

With Adaptive Query Execution enabled (`spark.sql.adaptive.enabled` is `true` by default on the session tested), Spark can replace its original plan after it sees the real size of the data.

Example: the join starts as a sort-merge join because Spark could not know that a filter would leave only a few rows on one side.

```python
big.join(other_big.filter("k < 100"), "k").collect()
```

The plan Spark printed after execution:

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   BroadcastHashJoin [k], [k], Inner, BuildRight
   :- AQEShuffleRead local
   :     +- Exchange hashpartitioning(k, 200)
   +- BroadcastExchange
      +- AQEShuffleRead local
            +- Exchange hashpartitioning(k, 200)
+- == Initial Plan ==
   SortMergeJoin [k], [k], Inner
   :- Sort → Exchange hashpartitioning(k, 200)
   +- Sort → Exchange hashpartitioning(k, 200)
```

Two things are worth noticing:

- The `Initial Plan` was a `SortMergeJoin`; the `Final Plan` is a `BroadcastHashJoin`. Spark changed its mind at runtime.
- The `Exchange` nodes are still there. Those shuffles had already run by the time Spark discovered that one side was small. What AQE saved here is the sort and the merge, and the data is read locally (`AQEShuffleRead local`).

For a broadcast chosen up front, the large side is never shuffled. For a broadcast chosen by AQE after the shuffle, the gain is smaller.

Databricks also exposes its own setting, `spark.databricks.adaptive.autoBroadcastJoinThreshold`, mentioned in a Databricks knowledge-base article. Check your Databricks Runtime documentation for its default before relying on a value.

---

## How to See Which Strategy Was Used

- `df.explain()` prints the physical plan. Look for the join node name.
- `df.explain("formatted")` prints a more readable plan.
- In the Spark UI, open the SQL / DataFrame tab for the query and inspect the plan graph.
- With AQE on, read the `Final Plan`, not only the `Initial Plan`. They can differ.

---

## How Joins Connect to Skew and Spill

| Strategy            | Skew risk                                                       | Spill risk                                               |
| ------------------- | --------------------------------------------------------------- | -------------------------------------------------------- |
| Broadcast Hash Join | Low on the large side, because it is not shuffled by the key    | Memory pressure if the broadcast side is too large       |
| Sort-Merge Join     | High: a hot key sends many rows to one partition                | Sort buffers can spill when a partition is large         |
| Shuffle Hash Join   | High: a skewed partition builds a very large hash table         | Hash table for the build side must fit in memory         |

- A join key with a hot value is the classic origin of data skew. See `DataSkew.md`.
- A skewed partition inside a sort or a hash table is a common reason for spilling. See `DataSpill.md`.
- When one side is genuinely small, broadcasting it removes the shuffle of the large side altogether, which avoids skew on that side.

---

## Common Mistakes

### 1. Confusing join type with join strategy

An `INNER JOIN` is not a broadcast join, and a broadcast join is not a type of join. One describes the result, the other describes the execution.

### 2. Broadcasting something that is not small

Raising `autoBroadcastJoinThreshold` or hinting `BROADCAST` on a large table can trade a shuffle for memory pressure on executors and the driver.

### 3. Assuming the hint always wins

As shown above, a hint on the wrong side of an outer join is silently ignored. Check the plan.

### 4. Looking only at the initial plan with AQE on

The strategy in the initial plan may not be the one that ran. Read the final plan.

### 5. Expecting a small table to always be broadcast

The decision uses size estimates. A Databricks knowledge-base article describes a case where a table below the threshold still was not broadcast because of empty partitions, and a case where the filtered size is what matters. If a broadcast does not happen, check the plan before changing settings.

---

## Quick Decision Guide

| Situation                                                           | Likely choice                                           |
| ------------------------------------------------------------------- | ------------------------------------------------------- |
| Large table joined with a small lookup table                        | Broadcast hash join                                     |
| Two large tables                                                    | Sort-merge join                                         |
| One side much smaller than the other, but too big to broadcast      | Consider a shuffle hash join, and test it               |
| Join condition without equality (`<`, `>`, `BETWEEN`)               | Broadcast nested loop join (expensive; look for a rewrite) |
| A hot key makes one task very slow                                  | Review skew handling (see `DataSkew.md`)               |
| Join stage shows large spill in the Spark UI                        | Review partition sizes and memory (see `DataSpill.md`) |

---

## Key Takeaways

- The join type decides which rows are returned. The join strategy decides how Spark computes them.
- To join, equal keys must meet on the same executor. Spark either broadcasts the small side or shuffles both sides.
- Broadcast hash join: no shuffle of the large side. Requires a genuinely small side, `10 MB` by default.
- Sort-merge join: shuffles and sorts both sides. The default workhorse for large-to-large joins.
- Shuffle hash join: shuffles both sides, no sort, builds a hash table. Best when one side is clearly smaller.
- Hints (`BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`) are requests. Spark may ignore them.
- AQE can change the strategy at runtime, so compare the initial and final plans.
- Always verify with `explain()` and the Spark UI instead of assuming.