"""Benchmark a partition-pruned query against the published sales table."""

from __future__ import annotations

import argparse
import logging
import time
from datetime import date

from pyspark.sql import SparkSession

LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", default="ecommerce_dw")
    parser.add_argument("--table", default="sales_fact")
    parser.add_argument("--date", help="event_date to benchmark (YYYY-MM-DD)")
    args = parser.parse_args()
    if args.date:
        try:
            date.fromisoformat(args.date)
        except ValueError as exc:
            parser.error(f"--date must use YYYY-MM-DD format: {exc}")
    return args


def run(args: argparse.Namespace) -> None:
    spark = (
        SparkSession.builder.appName("Performance_Comparison")
        .enableHiveSupport()
        .getOrCreate()
    )
    try:
        table = f"`{args.database}`.`{args.table}`"
        event_date = args.date or spark.sql(
            f"SELECT MIN(event_date) AS event_date FROM {table}"
        ).first()["event_date"]
        if event_date is None:
            raise RuntimeError(f"No event_date values found in {table}")

        query = f"""
            SELECT event, COUNT(*) AS event_count
            FROM {table}
            WHERE event_date = '{event_date}'
            GROUP BY event
        """
        started = time.perf_counter()
        result = spark.sql(query)
        rows = result.collect()
        elapsed = time.perf_counter() - started

        LOGGER.info("Benchmark date: %s", event_date)
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
