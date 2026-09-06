# Distributed Scaling Architecture Notes (EMR / Dataproc)

This document details how the local Spark execution model maps to an enterprise distributed production environment (e.g., AWS EMR or Google Cloud Dataproc).

## 1. Local Mode vs. Multi-Node Cluster Configuration

| Configuration Property | Local Mode Setting (Used Here) | Production Multi-Node (EMR / Dataproc) |
|---|---|---|
| `spark.master` | `local[*]` | `yarn` or `k8s` |
| `spark.sql.shuffle.partitions` | `16` (2x local CPU cores) | `200` to `2000` (depending on total target shuffle block sizes ~128MB each) |
| `spark.driver.memory` | `4g` | `8g` to `16g` |
| `spark.executor.memory` | In-process with driver | `16g` to `32g` per executor |
| `spark.executor.cores` | All available host threads | `4` to `5` cores per executor (sweet spot avoiding HDFS/IO contention) |
| Dynamic Allocation | Disabled | Enabled (`spark.dynamicAllocation.enabled=true`) with external shuffle service |

---

## 2. Shuffle Partition Tuning Rationale

- **Local Cluster Mode:**
  The default `spark.sql.shuffle.partitions = 200` creates severe overhead on a single machine processing 20M rows: 200 tasks mean 200 tiny files, disk IO context switching, and scheduler latency. We tuned this to `16` (2x host cores), ensuring each partition handles ~1.2M rows (~20–30MB uncompressed memory block), perfectly matching local parallelism without queue stalls.

- **Distributed Scaling Target (EMR/Dataproc at 100M - 1B Rows):**
  - **Rule of Thumb:** Target partition sizes between **100MB and 200MB** uncompressed post-shuffle.
  - At 100M+ rows (~20GB uncompressed), `spark.sql.shuffle.partitions` should be set to `150-200`.
  - At 1B+ rows (~200GB uncompressed), scale to `1000 - 2000` partitions.
  - Enable **Adaptive Query Execution (AQE)**:
    ```properties
    spark.sql.adaptive.enabled=true
    spark.sql.adaptive.coalescePartitions.enabled=true
    spark.sql.adaptive.skewJoin.enabled=true
    ```
    AQE dynamically merges undersized post-shuffle partitions and splits skewed partitions during runtime.

---

## 3. Broadcast Join Thresholds & Skew Avoidance

- **Dimension Broadcast:**
  The taxi zone lookup table contains only 265 rows (~10KB). Broadcasting this small table eliminates a full shuffle of the 20.3M row fact table, transforming an $O(N \log N)$ distributed sort-merge join into a direct $O(N)$ local map-side hash join.
- **Production Setting:** In production, keep `spark.sql.autoBroadcastJoinThreshold = 10485760` (10MB). Explicit broadcast calls (`broadcast(df)`) are maintained for mission-critical small dimension tables to avoid optimizer fallback.

---

## 4. Storage Partitioning Strategy

- We partition by **`pickup_date`** and **`pickup_borough`**.
- Partitioning by individual `PULocationID` (265 zones) would cause the **small file problem** (thousands of sub-1MB files leading to object storage API throttle and metastore degradation).
- Grouping at the Borough level (~6 distinct partitions per date) produces well-sized 20MB–100MB Parquet part files, optimal for cloud object stores (S3/GCS) and downstream analytics engines (Athena, Snowflake, DuckDB).
