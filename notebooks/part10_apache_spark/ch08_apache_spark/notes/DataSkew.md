# Data Skew

When **a single partition** determines how long the entire job takes.

---

## Partitions

A partition is a **logical chunk of data** that Spark can process independently and in parallel.

For example:

* 100 million rows → 10 partitions
* Each partition → approximately 10 million rows

### Key ideas

* **Independent:** Each partition contains a portion of the data and can usually be processed independently of the other partitions.
* **Parallel:** Spark can process multiple partitions at the same time using different CPU cores or machines in a cluster.
* **Unit of work:** A partition is the basic unit of data that Spark assigns to a **task**. A task processes one partition for a particular operation.
* **Physical vs. logical:** A partition is a logical division of a dataset, but Spark maps those partitions to physical resources such as CPU cores and executor machines when processing the data.

### Simple example

Imagine 100 million rows divided into 10 partitions:

```text
Dataset
├── Partition 1 → 10M rows → Task 1
├── Partition 2 → 10M rows → Task 2
├── Partition 3 → 10M rows → Task 3
├── ...
└── Partition 10 → 10M rows → Task 10
```

If the cluster has enough available resources, several tasks can run **simultaneously**, allowing Spark to process the dataset much faster than processing all 100 million rows as one large block.

---

## What is a Task?

A **task** is a unit of work that Spark performs on **one partition**.

Think of it this way:

* **Partition = data** → A portion of the dataset.
* **Task = work** → The operation Spark performs on that partition.

If a DataFrame has **10 partitions**, a Spark stage will typically create **10 tasks** for an operation that processes every partition.

### Example

```python
df = spark.read.parquet("s3://sales/")
df.rdd.getNumPartitions()
# 10
```

This means the data is divided into **10 partitions**.

If Spark needs to perform an operation on all of them:

```text
Partition 1 → Task 1
Partition 2 → Task 2
Partition 3 → Task 3
...
Partition 10 → Task 10
```

The tasks can run **in parallel** if enough executor cores are available.

### Important distinction

A task is **not the same as a partition**:

> **Partition = the data Spark needs to process.**
> **Task = the work Spark performs on that data.**

A useful mental model is:

```text
Dataset
   ↓
Partitions
   ↓
Tasks
   ↓
Executor cores
```

One task processes one partition for a particular **stage**. The same partition may therefore be processed by different tasks in different stages.

---

## What if the partitions aren't equal in size?

Partitions do not always contain the same amount of data. This can affect how long tasks take to finish.

Imagine Spark creates **4 tasks** running the same operation:

### Balanced partitions

Each task receives a similar amount of data:

```text
Task 1 → 10 GB
Task 2 → 10 GB
Task 3 → 10 GB
Task 4 → 10 GB
```

The tasks have a similar workload, so they usually finish at roughly the same time.

### Unbalanced partitions

Now imagine:

```text
Task 1 → 1 GB
Task 2 → 1 GB
Task 3 → 1 GB
Task 4 → 36 GB
```

Tasks 1–3 finish quickly, but Task 4 has much more data to process.

Even though Spark is using **4 tasks**, the overall operation may have to wait for Task 4 to finish.

> **The problem is not necessarily the total amount of work. The problem is that the work is distributed unevenly.**

This situation is called **data skew**.

---

## What is Data Skew?

**Data skew** occurs when data is distributed very unevenly across partitions during distributed processing.

In other words:

> **Some partitions contain significantly more data than others.**

For example:

```text
Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 36 GB  ← skewed partition
```

Spark may assign one task to each partition:

```text
Partition 1 → Task 1 → finishes quickly
Partition 2 → Task 2 → finishes quickly
Partition 3 → Task 3 → finishes quickly
Partition 4 → Task 4 → takes much longer
```

### How does data skew arise?

Data skew is especially common during **shuffle operations**, where Spark redistributes data across partitions based on a key.

Common operations that can cause or expose skew include:

* `groupBy()`
* `join()`
* `distinct()`
* `reduceByKey()`
* `orderBy()`

For example, if you join data using `customer_id` and one customer appears in a huge number of records, many records may end up in the same partition.

### How does data skew manifest?

The main symptom is an **uneven task workload**:

```text
Task 1 → 2 seconds
Task 2 → 2 seconds
Task 3 → 3 seconds
Task 4 → 5 minutes  ← bottleneck
```

Most tasks finish quickly, while one or a few tasks continue running.

### Why is data skew a problem?

Spark's parallelism is limited by the **slowest tasks** in a stage.

If 99 tasks finish but 1 task is still processing a huge partition, the stage cannot fully complete until that remaining task finishes.

> **Data skew creates a bottleneck because a small number of tasks have significantly more work than the others.**

### Simple mental model

```text
Balanced:
10 GB → 10 GB → 10 GB → 10 GB
  ↓       ↓       ↓       ↓
Task 1  Task 2  Task 3  Task 4
  ↓       ↓       ↓       ↓
Similar completion times


Skewed:
1 GB → 1 GB → 1 GB → 36 GB
 ↓      ↓      ↓       ↓
 T1     T2     T3      T4
 ↓      ↓      ↓       ↓
Fast   Fast   Fast   Slow ← bottleneck
```

**Partition imbalance → uneven task workload → slow task → stage bottleneck.**

---

## Skew Arises During the Shuffle

A **shuffle** happens when Spark needs to redistribute records across partitions so that records with the same key can be processed together.

For example:

```python
df.groupBy("customer").count()
```

Spark needs to bring all records for the same customer into the same partition so it can calculate the count.

Suppose the data contains:

```text
Customer A → 10 records
Customer B → 20 records
Customer C → 8,000,000 records
```

During the shuffle, Spark distributes the records based on the `customer` key:

```text
Customer A ─┐
Customer B ─┼──→ Partition 1
Customer C ─┼──→ Partition 2
            │
            └──→ 8,000,000 records
```

If Customer C's records are assigned to the same partition, that partition becomes much larger than the others.

```text
Partition 1 → 100 MB
Partition 2 → 8 GB    ← skewed
Partition 3 → 120 MB
Partition 4 → 90 MB
```

The task processing Partition 2 now has significantly more work.

---

## Why Does Spark Put the Same Key Together?

For operations such as `groupBy`, Spark needs records with the same key to end up in the same partition.

A simplified way to think about the partitioning is:

```text
partition = hash(customer) % number_of_partitions
```

For example, with 4 partitions:

```text
hash("Customer A") % 4 → Partition 0
hash("Customer B") % 4 → Partition 2
hash("Customer C") % 4 → Partition 1
```

Every record with `"Customer C"` produces the same hash and therefore goes to the same partition.

> This is why a very frequent key can create a **hot partition**: one partition receives a disproportionately large amount of data.

The actual implementation is more sophisticated than this simplified formula, but it is a useful mental model for understanding Spark's hash partitioning.

---

## Skew Doesn't Necessarily Exist in the Original Data

**Data skew can be created or amplified by the shuffle.**

The original data may be relatively well balanced across its input partitions:

```text
Before shuffle:

Partition 1 → 1 GB
Partition 2 → 1 GB
Partition 3 → 1 GB
Partition 4 → 1 GB
```

Then a `groupBy("customer")` redistributes the records based on the customer key:

```text
After shuffle:

Partition 1 → 0.8 GB
Partition 2 → 3.5 GB
Partition 3 → 0.7 GB
Partition 4 → 8.0 GB  ← skew
```

The original files did not necessarily contain an 8 GB partition.

**The shuffle created a new distribution of the data.**

### Important distinction

The **data itself** may contain a skewed key distribution:

```text
Customer A → 10 records
Customer B → 20 records
Customer C → 8,000,000 records
```

The **shuffle** is what redistributes those records into partitions according to the key.

So the more precise explanation is:

> **Skew is caused by an uneven distribution of key values, and a shuffle can expose that skew by concentrating records with the same key into the same partition.**

This distinction is important because skew is not simply "one partition was large in the input." It is often a **partitioning problem created during redistribution**.

### Mental model

```text
Original data
     ↓
Input partitions may be balanced
     ↓
groupBy / join / distinct
     ↓
  SHUFFLE
     ↓
Redistribute records by key
     ↓
One key has millions of records
     ↓
Many records go to the same partition
     ↓
Large partition
     ↓
Slow task
     ↓
Bottleneck
```

---

## Why Does the Whole Job Slow Down?

A Spark **stage** cannot be considered complete until all of its required tasks finish.

For example, with 8 tasks:

```text
Task 1 → 2 sec
Task 2 → 2 sec
Task 3 → 3 sec
Task 4 → 2 sec
Task 5 → 2 sec
Task 6 → 3 sec
Task 7 → 2 sec
Task 8 → 10 min  ← skewed task
```

Seven tasks finish quickly, but the stage still has to wait for Task 8.

> **The stage progresses at the speed of its slowest remaining task.**

Meanwhile, some executor cores may become idle because there is no additional work for them to process within that stage.

---

## The Partition Is Indivisible

A **partition is processed by one task at a time**.

If a partition contains 8 GB of data, Spark does not normally split that single partition among several cores during that task:

```text
8 GB partition
      ↓
   Task 1
      ↓
  One core
```

It cannot simply do this:

```text
8 GB partition
   ↓       ↓
Core 1   Core 2
```

Instead, the partition is the task's unit of input.

This is why a very large partition can become a **bottleneck**: even if other cores are available, they cannot directly share the work of that task.

> **Parallelism happens primarily across partitions, not within a single partition.**

---

## Salting: Breaking Up a Hot Key

**Salting** is a technique used to reduce data skew by adding an artificial value to a frequently occurring key.

The goal is to turn one **hot key** into several different keys so its records can be distributed across multiple partitions.

### Without salting

Suppose Customer C has 8 million records:

```text
customer_id
    ↓
Customer C
    ↓
8,000,000 records
    ↓
One partition
    ↓
One task
```

That partition becomes a bottleneck.

### With salting

Add a salt value, for example a random number from `0` to `3`:

```text
(customer_id, salt)
```

Customer C's records become:

```text
(Customer C, 0)
(Customer C, 1)
(Customer C, 2)
(Customer C, 3)
```

Now the records can be distributed across multiple partitions:

```text
Customer C + 0 → Partition 1 → Task 1
Customer C + 1 → Partition 2 → Task 2
Customer C + 2 → Partition 3 → Task 3
Customer C + 3 → Partition 4 → Task 4
```

Instead of one task processing all 8 million records:

```text
             8M records
                  ↓
             One task ❌
```

the work can be distributed:

```text
       8M records
           ↓
   ┌───────┼───────┬───────┐
   ↓       ↓       ↓       ↓
  2M      2M      2M      2M
   ↓       ↓       ↓       ↓
 Task 1  Task 2  Task 3  Task 4
```

This allows the workload to be processed **in parallel**.

### Important detail

Salting changes the grouping key, so for operations such as `groupBy`, you generally need an **additional aggregation step** afterward to combine the salted results back into the original key.

For example:

```text
Original:
groupBy(customer)

Salting:
groupBy(customer, salt)
        ↓
partial results
        ↓
groupBy(customer)
        ↓
final result
```

> **Salting does not reduce the amount of data. It changes how the data is distributed so that a hot key can be processed by multiple tasks instead of becoming concentrated in one partition.**

