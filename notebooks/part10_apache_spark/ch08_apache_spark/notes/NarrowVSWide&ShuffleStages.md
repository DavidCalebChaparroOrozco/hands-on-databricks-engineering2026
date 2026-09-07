# Narrow vs Wide Transformations, Shuffle & Stages

## Four Linked Concepts

* **Narrow Transformation:** Each output partition can be created using data from **only one input partition**. No data needs to be moved between partitions.

* **Wide Transformation:** An output partition needs data from **multiple input partitions**. This requires data to be redistributed across partitions.

* **Shuffle:** The process in which Spark **moves and redistributes data between partitions**, usually across different executors, to perform a wide transformation.

* **Stages:** Groups of tasks that Spark can execute together without needing to redistribute data. A **shuffle creates a boundary between stages**.

> **The complete chain:** A **narrow transformation** does not require data movement → a **wide transformation** requires data from multiple partitions → this causes a **shuffle** → and the shuffle divides the job into separate **stages**.

---

## What is a **Narrow Transformation**

A **narrow transformation** is a transformation where **each output partition depends on data from only one input partition**.

Spark can process each partition independently because **no data needs to be moved between partitions**.

**Examples:**

* `filter()`
* `select()`
* `withColumn()`
* `map()`

**Dependency:** 1 → 1

* Input Partition 1 → Output Partition 1
* Input Partition 2 → Output Partition 2
* Input Partition 3 → Output Partition 3

> **No output partition needs data from another input partition, so no shuffle is required.**

### Example: Narrow Transformation

```python
df.filter(df["age"] > 18)
```

Imagine the DataFrame is divided into three partitions:

```text
Partition 1 → [Alice, 25] [Bob, 16]
Partition 2 → [Carol, 30] [David, 17]
Partition 3 → [Emma, 22] [Frank, 15]
```

Spark processes each partition **independently**:

```text
Partition 1 → filter age > 18 → [Alice, 25]
Partition 2 → filter age > 18 → [Carol, 30]
Partition 3 → filter age > 18 → [Emma, 22]
```

### Why is this a Narrow Transformation?

Each partition receives its own data, applies the filter, and produces its output **without needing data from another partition**.

```text
Input Partition 1 → Output Partition 1
Input Partition 2 → Output Partition 2
Input Partition 3 → Output Partition 3
```

> Each partition processes its data locally. **No data is moved between partitions, so no shuffle is required.**

---

### Does Not Combine Data from Different Partitions

Each partition is processed **completely independently**, so Spark does not need to combine data from different partitions.

Because each partition is self-contained, Spark **does not need to move data between partitions or across the network**. Therefore, **no shuffle is required**.

This is why narrow transformations are generally **faster and less expensive** than transformations that require a shuffle.

### Typical Examples

* **`filter()`**: Removes rows that do not meet a condition.
* **`select()`**: Keeps only the columns you specify.
* **`withColumn()`**: Adds or modifies a column using calculations on each row.
* **`map()`**: Applies a function independently to each element.

> **Key idea:** Each partition can be processed on its own → no data exchange between partitions → no shuffle.

---

## What is a **Wide Transformation**

A **wide transformation** occurs when **an output partition needs data from multiple input partitions**.

The key idea is that Spark must **redistribute data between partitions** to produce the result.

This redistribution is called a **shuffle**, and it can be expensive because Spark may need to move data **across the network**.

### N → 1 Dependency

Multiple input partitions can contribute data to the same output partition:

```text
Input Partition 1 ──┐
                    │
Input Partition 2 ──┼──→ Output Partition 1
                    │
Input Partition 3 ──┘
```

For example, with:

```python
df.groupBy("department").count()
```

Employees from different partitions may belong to the same department. Spark must bring those rows together so it can calculate the count.

> **Key idea:** Multiple input partitions → data must be redistributed → shuffle → output partitions are created from data coming from multiple input partitions.

---

### Example: Wide Transformation with `groupBy()`

When you use `groupBy()`, Spark must bring records with the **same grouping key** together.

```python
df.groupBy("country").count()
```

For example, imagine the data is distributed across three partitions:

```text
Partition 1: 🇨🇴 Colombia   🇺🇸 USA        🇨🇴 Colombia
Partition 2: 🇧🇷 Brazil     🇨🇴 Colombia   🇺🇸 USA
Partition 3: 🇺🇸 USA        🇧🇷 Brazil     🇨🇴 Colombia
```

To calculate the count for each country, Spark needs all records for the same country in the appropriate output partition:

```text
Partition 1 ──┐
Partition 2 ──┼──→ Shuffle → Grouped by country
Partition 3 ──┘
```

So records may have to **leave their original partitions and move to other partitions**.

> ⚠️ The records for `Colombia` are spread across all three input partitions. Spark cannot calculate the total count for `Colombia` using only one partition—it must receive the relevant records from the others.

### Key Idea

```text
Same key → must be brought together
         → data moves between partitions
         → shuffle
         → wide transformation
```

The idea of **“9 records → 9 trips”** is useful as a simple mental model, but it is not literally how Spark works: Spark **does not necessarily make one network trip per record**. It typically partitions and transfers data in batches.

---

## Moving Data Costs

A **shuffle is expensive** because Spark has to redistribute data between partitions. Every wide transformation that requires a shuffle can introduce several costs:

* **Data redistribution:** Spark reorganizes records according to their grouping or partitioning key so that related records end up together.
* **Network transfer:** Data may need to move between executors, often across different machines in the cluster.
* **Temporary disk I/O:** Shuffle data may be written to local disk and later read again.
* **Serialization and deserialization:** Spark may need to convert data into a format suitable for transferring or storing it, and then convert it back.
* **Stage boundary:** A shuffle creates a boundary in Spark's execution plan, dividing the work into separate stages.

### Friction

A shuffle involves several components working together:

**serialization → network transfer → disk I/O → deserialization**

Each additional step introduces overhead and potential bottlenecks.

### Relative Cost

```text
Narrow transformation
filter()
    ↓
Process data locally
    ↓
No shuffle
    ↓
Lower cost
```

```text
Wide transformation
groupBy()
    ↓
Redistribute data
    ↓
Shuffle
    ↓
Network + serialization + disk I/O
    ↓
Higher cost
```

> **Key idea:** Narrow transformations usually keep data local, while wide transformations may require expensive data movement and processing across the cluster.

---

## What is a **Shuffle**

A **shuffle** is the process Spark uses to **redistribute data between partitions**.

It occurs when a **wide transformation** requires data from multiple input partitions to be brought together.

### What Triggers a Shuffle?

Common operations that can require a shuffle include:

* **`groupBy()`**: Brings records with the same grouping key together.
* **`join()`**: Brings matching records from two datasets together.
* **`distinct()`**: Brings records together to identify and remove duplicates.
* **`orderBy()`**: Redistributes data so it can be globally sorted.

### Simple Example

```python
df.groupBy("country").count()
```

If `Colombia` records are spread across several partitions:

```text
Partition 1 → Colombia
Partition 2 → Colombia
Partition 3 → Colombia
```

Spark must redistribute those records so they can be processed together:

```text
Partition 1 ──┐
Partition 2 ──┼──→ Shuffle → Records grouped by country
Partition 3 ──┘
```

> **Key idea:** Wide transformation requires data from multiple partitions → Spark redistributes the data → **shuffle**.

A shuffle is often one of the **most expensive parts of a Spark job** because it can involve network transfer, serialization, disk I/O, and additional processing.

---

## Shuffle in Action

During a **shuffle**, Spark uses the record's **key** to determine which partition should receive it.

For example:

```text
Colombia → Partition 1
USA      → Partition 2
Mexico   → Partition 3
```

Records are redistributed based on their key:

```text
Input Partitions          Shuffle          Output Partitions

P1 ── Colombia ───────────────┐
P2 ── Colombia ───────────────┼──→ P1: All Colombia records
P3 ── Colombia ───────────────┘

P1 ── USA ────────────────────┐
P2 ── USA ────────────────────┼──→ P2: All USA records
P3 ── USA ────────────────────┘

P1 ── Mexico ─────────────────┐
P2 ── Mexico ──────────────────┼──→ P3: All Mexico records
P3 ── Mexico ──────────────────┘
```

After the shuffle, **all records with the same key are placed in the same partition**.

This allows Spark to perform the next operation **locally within each partition**.

For example:

```python
df.groupBy("country").count()
```

Once all `Colombia` records are together, Spark can simply count them in that partition.

> **Key idea:** The shuffle redistributes records by key → records with the same key end up in the same partition → the next operation can be performed locally.

---

## What Is a Stage?

A **stage** is a block of work that Spark can execute together **without needing to redistribute data between partitions**.

### Exam Definition

> **A stage groups together all the transformations that can be performed without moving data between partitions.**

Spark can execute a sequence of **narrow transformations** in the same stage because they do not require a shuffle.

### Example

```text
STAGE 1
filter()
   ↓
select()
   ↓
withColumn()
   ↓
SHUFFLE
   ↓
STAGE 2
groupBy()
   ↓
count()
```

In **Stage 1**, Spark can execute:

```text
filter() → select() → withColumn()
```

These are narrow transformations, so data stays within its partitions.

When `groupBy()` requires a **shuffle**, Spark must redistribute the data. This creates a **boundary between stages**.

After the shuffle, Spark starts **Stage 2**:

```text
groupBy() → count()
```

> **Key idea:** Narrow transformations can run together in the same stage. A **shuffle marks the boundary** between one stage and the next.

---

## Spark Cuts the Plan Where There Is a Shuffle

Consider this code:

```python
df.filter(df["age"] > 18) \
  .select("name", "age") \
  .groupBy("age") \
  .count()
```

Spark divides the execution plan into stages at the **shuffle boundary**.

```text
STAGE 1
filter()
   ↓
select()
   ↓
SHUFFLE
   ↓
STAGE 2
groupBy()
   ↓
count()
```

### Why?

* **`filter()`** is a narrow transformation → no shuffle.
* **`select()`** is a narrow transformation → no shuffle.
* **`groupBy()`** requires data to be redistributed → shuffle.
* **`count()`** operates after the grouping and can be performed within the resulting partitions.

Therefore:

> **`filter()` + `select()` → Stage 1 → shuffle → `groupBy()` + `count()` → Stage 2**

**Key idea:** Spark can execute consecutive narrow transformations in the same stage. **The shuffle creates the boundary that separates stages.**

---

## A Production Line

Think of a Spark job like a **production line**. Spark chains narrow transformations together and keeps processing as long as it does not need to redistribute data.

```text
Raw Data
   ↓
STAGE 1
filter()      {narrow}
   ↓
select()      {narrow}
   ↓
withColumn()  {narrow}
   ↓
SHUFFLE
   ↓
STAGE 2
groupBy()     {wide → requires shuffle}
   ↓
count()       {local operation}
   ↓
Result
```

### How the Production Line Works

* As long as the transformations are **narrow**, Spark can keep them in the **same stage**.
* When a transformation requires a **shuffle**, Spark must stop that part of the pipeline and redistribute the data.
* After the shuffle, Spark continues the remaining work in a **new stage**.

> **Key idea:** Narrow transformations keep the production line moving → a shuffle stops the line and redistributes the data → the next part of the work continues in a new stage.

---

## Four Words You Shouldn't Mix

Each concept answers a **different question**:

### 1. Transformation — **What do I write?**

A **transformation** is an operation you define on a DataFrame, such as:

```python
filter()
select()
withColumn()
groupBy()
```

It describes **what you want Spark to do with the data**.

---

### 2. Narrow / Wide — **What does it need?**

**Narrow** and **wide** describe the **dependency between input and output partitions**.

* **Narrow:** An output partition needs data from only one input partition.
* **Wide:** An output partition needs data from multiple input partitions.

In other words:

> **Does this operation need data from other partitions?**

---

### 3. Shuffle — **What happens physically?**

A **shuffle** is the **physical redistribution of data between partitions**.

It happens when Spark needs to move data so that records can be processed together, typically because of a **wide dependency**.

> **Wide dependency → shuffle is required.**

---

### 4. Stage — **How is the work organized?**

A **stage** is a group of operations that Spark can execute together **without crossing a shuffle boundary**.

When Spark encounters a shuffle, the current stage ends and a **new stage begins** after the shuffle.

```text
Transformation
      ↓
Narrow or Wide?
      ↓
If Wide → Shuffle
      ↓
Shuffle creates a Stage boundary
```

### The Complete Picture

```text
You write a transformation
        ↓
Is it narrow or wide?
        ↓
Narrow → no shuffle → stays in the same stage
        ↓
Wide → requires shuffle
        ↓
Shuffle → stage boundary
        ↓
New stage
```

> **Transformation = what you write**
> 
> **Narrow/Wide = what the operation needs**
> 
> **Shuffle = data movement that occurs because of a wide dependency**
> 
> **Stage = how Spark groups the work around shuffle boundaries**