from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, from_unixtime

print("=" * 80)
print("ETL PIPELINE START")
print("=" * 80)

# Initialize Spark
spark = SparkSession.builder \
    .appName("Ecommerce_Distributed_ETL") \
    .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .enableHiveSupport() \
    .getOrCreate()

print("\nReading data from HDFS...")

df_category = spark.read.csv(
    "hdfs://localhost:9000/data/ecommerce/raw/categories/category_tree.csv",
    header=True, inferSchema=True
)

df_items_1 = spark.read.csv(
    "hdfs://localhost:9000/data/ecommerce/raw/items/item_properties_part1.csv",
    header=True, inferSchema=True
)

df_items_2 = spark.read.csv(
    "hdfs://localhost:9000/data/ecommerce/raw/items/item_properties_part2.csv",
    header=True, inferSchema=True
)

df_events = spark.read.csv(
    "hdfs://localhost:9000/data/ecommerce/raw/events/events.csv",
    header=True, inferSchema=True
)

print("Data Cleaning...")
df_clean = df_events.dropna(subset=["timestamp", "visitorid", "itemid", "event"])
df_clean = df_clean.dropDuplicates(["timestamp", "visitorid", "itemid"])

print("Transformation...")
df_final = df_clean.withColumn(
    "event_date",
    to_date(from_unixtime(col("timestamp") / 1000))
)

df_final = df_final.select(
    "timestamp",
    "visitorid",
    "event",
    "itemid",
    "transactionid",
    "event_date"
)

print("Loading into Hive...")

spark.sql("DROP DATABASE IF EXISTS ecommerce_dw CASCADE")
spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce_dw")

df_final.write \
    .mode("overwrite") \
    .format("parquet") \
    .saveAsTable("ecommerce_dw.sales_fact")

print("ETL COMPLETED")

spark.stop()