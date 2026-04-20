from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws
import subprocess

spark = SparkSession.builder \
    .appName("Load_to_HBase") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE ecommerce_dw")

df = spark.sql("""
SELECT timestamp, visitorid, event, itemid,
COALESCE(transactionid, 'null') as transactionid, event_date
FROM sales_fact
LIMIT 10000
""")

df_hbase = df.withColumn(
    "rowkey",
    concat_ws("_", col("timestamp"), col("visitorid"), col("itemid"))
)

records = df_hbase.collect()

hbase_script = []
hbase_script.append("# HBase Bulk Insert Script")

for row in records:
    rowkey = row['rowkey']
    hbase_script.append(f"put 'realtime_sales', '{rowkey}', 'info:event', '{row['event']}'")

hbase_script.append("exit")

with open('/tmp/hbase_insert.txt', 'w') as f:
    f.write('\n'.join(hbase_script))

subprocess.run(['hbase', 'shell', '/tmp/hbase_insert.txt'])

spark.stop()