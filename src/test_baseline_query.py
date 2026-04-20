from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Baseline_Query") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE ecommerce_dw")

sample_date = spark.sql("SELECT event_date FROM sales_fact LIMIT 1").collect()[0]['event_date']

query = f"""
SELECT event, COUNT(*) as count
FROM sales_fact_raw
WHERE event_date = '{sample_date}'
GROUP BY event
"""

start_time = time.time()
result = spark.sql(query)
result.show()
end_time = time.time()

print(f"Runtime: {end_time - start_time:.2f} seconds")

spark.stop()