# E-Commerce Big Data Pipeline

An end-to-end batch analytics pipeline for clickstream data, built with
**Apache Spark, Hadoop HDFS, Hive, Parquet, and HBase**. The project
demonstrates how to turn raw event data into a partitioned analytical
dataset, reusable SQL insights, and a bounded low-latency serving view.

## Why this project

This repository is intentionally focused on the engineering decisions that
matter in a production-style data platform:

- explicit input schemas instead of relying on schema inference;
- idempotent, parameterized Spark jobs;
- partitioned Parquet storage for date-based pruning;
- a separate HBase serving path for recent event lookups;
- repeatable benchmark scripts with real Spark actions;
- SQL examples that run against the published warehouse table.

## Architecture

```text
                         +----------------------+
                         |  Raw CSV files        |
                         |  events.csv           |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | HDFS raw zone        |
                         +----------+-----------+
                                    |
                                    v
                         +----------------------+
                         | Spark ETL             |
                         | schema, quality,     |
                         | deduplication        |
                         +----------+-----------+
                                    |
                                    v
              +---------------------+---------------------+
              |                                           |
              v                                           v
 +---------------------------+                +---------------------------+
 | Hive + partitioned        |                | HBase serving sample      |
 | Parquet: sales_fact       |                | bounded recent lookups   |
 +-------------+-------------+                +---------------------------+
               |
               v
 +---------------------------+
 | Hive/Spark SQL analytics  |
 | date-partition pruning    |
 +---------------------------+
```

The current ETL publishes one fact table, `sales_fact`. The source layout
also reserves locations for category and item-property files so dimensions
can be added without changing the ingestion boundary.

| Layer | Technology | Responsibility |
| --- | --- | --- |
| Storage | HDFS | Durable storage for raw and processed data |
| Compute | Apache Spark | Cleaning, deduplication, transformation, and writes |
| Warehouse | Hive + Parquet | Partitioned analytical table and SQL access |
| Serving | HBase | Fast key-based access to a bounded event sample |

## Data model

The ETL publishes `ecommerce_dw.sales_fact` with this schema:

| Column | Type | Description |
| --- | --- | --- |
| `timestamp` | `bigint` | Event time in milliseconds since Unix epoch |
| `visitorid` | `bigint` | Anonymous visitor identifier |
| `event` | `string` | Event type, such as `view`, `addtocart`, or `transaction` |
| `itemid` | `bigint` | Product identifier |
| `transactionid` | `string` | Optional transaction identifier |
| `event_date` | `date` | Derived partition column |

The table is stored as Parquet and partitioned by `event_date`. The included
SQL examples are in [`sql/analytics_queries.sql`](sql/analytics_queries.sql).

## Prerequisites

- Python 3.9+
- Java 8, 11, or 17
- Apache Spark with Hive support
- Hadoop/HDFS
- Hive metastore support
- HBase (only required for the serving step)

Install the Python dependency:

```bash
python -m pip install -r requirements.txt
```

The scripts default to a local HDFS deployment at
`hdfs://localhost:9000`. Every important path and table name can be
overridden with a command-line option.

## Quick start

### 1. Prepare the HDFS layout

Place the source files at:

```text
/data/ecommerce/raw/events/events.csv
/data/ecommerce/raw/categories/category_tree.csv
/data/ecommerce/raw/items/item_properties_part1.csv
/data/ecommerce/raw/items/item_properties_part2.csv
```

Only `events.csv` is required by the current ETL job; the other files are
kept in the layout for the broader warehouse model and future dimensions.

### 2. Run the ETL

```bash
spark-submit src/ecommerce_etl_hdfs.py \
  --hdfs-root hdfs://localhost:9000/data/ecommerce
```

The job fails safely if `ecommerce_dw.sales_fact` already exists. To
intentionally replace it:

```bash
spark-submit src/ecommerce_etl_hdfs.py --overwrite
```

The default behavior avoids dropping the entire database, which protects
unrelated tables and makes reruns explicit.

### 3. Run analytics

```bash
beeline -e "USE ecommerce_dw; SELECT event, COUNT(*) FROM sales_fact GROUP BY event;"
```

For more examples, run the statements in
[`sql/analytics_queries.sql`](sql/analytics_queries.sql).

### 4. Load a serving sample into HBase

```bash
spark-submit src/load_to_hbase.py \
  --hbase-table realtime_sales \
  --limit 10000
```

The loader streams the bounded Spark result to the driver, escapes values
for HBase shell syntax, removes its temporary script, and fails loudly if
the HBase command returns a non-zero exit code.

## Performance experiment

Run the full-table baseline and the partition-pruned query separately:

```bash
spark-submit src/test_baseline_query.py
spark-submit src/performance_comparison.py
```

The optimized query filters on `event_date`, which allows Spark/Hive to
prune unrelated partitions. Benchmark numbers depend on cluster size, file
layout, caching, and input volume. The optimized query should reduce work
when its `event_date` filter selects fewer partitions, but the exact
improvement must be measured on the target cluster.

## Repository layout

```text
.
├── hbase/                        # HBase schema and tuning notes
├── sql/                          # Warehouse analytics
├── src/
│   ├── ecommerce_etl_hdfs.py     # Raw events -> partitioned Hive table
│   ├── load_to_hbase.py          # Hive table -> HBase serving sample
│   ├── performance_comparison.py # Partition-pruned benchmark
│   └── test_baseline_query.py    # Full-table benchmark
├── requirements.txt
└── README.md
```

## Engineering notes

- **Data quality:** rows missing `timestamp`, `visitorid`, `itemid`, or
  `event` are rejected; duplicate event keys are removed.
- **Repeatability:** jobs expose CLI parameters and use structured logging
  instead of hard-coded print statements.
- **Safety:** the ETL does not drop databases, and overwrite behavior is
  opt-in.
- **Scalability:** Parquet and date partitioning reduce IO for time-bounded
  queries; HBase loading is explicitly capped with `--limit`.
- **Operational clarity:** Spark sessions are stopped in `finally` blocks,
  and external command failures are surfaced to the caller.

## Limitations and next steps

This is a portfolio-scale reference implementation, not a turnkey
production deployment. A production rollout would add orchestration,
schema/data-quality metrics, incremental ingestion, secrets/configuration
management, automated integration tests, and an HBase bulk-load path for
very large serving datasets.

## License

No license has been selected yet. Add a license before distributing this
repository for reuse.
