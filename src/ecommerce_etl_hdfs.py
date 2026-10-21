"""Clean the raw e-commerce events and publish a partitioned Hive table."""

from __future__ import annotations

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date
from pyspark.sql.types import LongType, StringType, StructField, StructType

LOGGER = logging.getLogger(__name__)

DEFAULT_HDFS_ROOT = "hdfs://localhost:9000/data/ecommerce"
DEFAULT_WAREHOUSE = "hdfs://localhost:9000/user/hive/warehouse"
DATABASE = "ecommerce_dw"
TABLE = "sales_fact"

EVENT_SCHEMA = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("visitorid", LongType(), True),
        StructField("event", StringType(), True),
        StructField("itemid", LongType(), True),
        StructField("transactionid", StringType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hdfs-root", default=DEFAULT_HDFS_ROOT)
    parser.add_argument("--warehouse", default=DEFAULT_WAREHOUSE)
    parser.add_argument("--database", default=DATABASE)
    parser.add_argument("--table", default=TABLE)
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace the target table. By default, fail if it already exists.",
    )
    return parser.parse_args()


def read_events(spark: SparkSession, hdfs_root: str) -> DataFrame:
    return spark.read.csv(
        f"{hdfs_root}/raw/events/events.csv",
        header=True,
        schema=EVENT_SCHEMA,
    )


def transform_events(events: DataFrame) -> DataFrame:
    required_columns = ["timestamp", "visitorid", "itemid", "event"]
    return (
        events.dropna(subset=required_columns)
        .dropDuplicates(["timestamp", "visitorid", "itemid", "event"])
        .withColumn(
            "event_date",
            to_date(from_unixtime(col("timestamp") / 1000)),
        )
        .select(
            "timestamp",
            "visitorid",
            "event",
            "itemid",
            "transactionid",
            "event_date",
        )
    )


def build_spark(app_name: str, warehouse: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.warehouse.dir", warehouse)
        .enableHiveSupport()
        .getOrCreate()
    )


def run(args: argparse.Namespace) -> None:
    spark = build_spark("Ecommerce_Distributed_ETL", args.warehouse)
    try:
        target = f"{args.database}.{args.table}"
        if spark.catalog.tableExists(target) and not args.overwrite:
            raise RuntimeError(
                f"Target table {target} already exists; pass --overwrite to replace it."
            )

        LOGGER.info("Reading raw events from %s", args.hdfs_root)
        events = read_events(spark, args.hdfs_root)
        cleaned = transform_events(events)

        spark.sql(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")
        writer = (
            cleaned.write.mode("overwrite" if args.overwrite else "error")
            .format("parquet")
            .partitionBy("event_date")
        )
        LOGGER.info("Writing partitioned table %s", target)
        writer.saveAsTable(target)
        LOGGER.info("ETL completed successfully")
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run(parse_args())
