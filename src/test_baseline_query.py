"""Run a repeatable full-table baseline for comparison with the partitioned query."""

from __future__ import annotations

import argparse
import logging
import time

from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="ecommerce_dw")
    parser.add_argument("--table", default="sales_fact")
    return parser.parse_args()


def run(args: argparse.Namespace) -> None:
    spark = (
        SparkSession.builder.appName("Baseline_Query")
        .enableHiveSupport()
        .getOrCreate()
    )
    try:
        table = f"`{args.database}`.`{args.table}`"
        query = f"""
            SELECT event, COUNT(*) AS event_count
            FROM {table}
            GROUP BY event
        """
        started = time.perf_counter()
        result = spark.sql(query)
        rows = result.collect()
        elapsed = time.perf_counter() - started

        LOGGER.info("Rows returned: %d", len(rows))
        LOGGER.info("Runtime: %.2f seconds", elapsed)
        result.show(truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    run(parse_args())
