from pyspark.sql import SparkSession
import time

spark = SparkSession.builder \
    .appName("Performance_Comparison") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sql("USE ecommerce_dw")

sample_date = spark.sql("SELECT event_date FROM sales_fact LIMIT 1").collect()[0]['event_date']

optimized_query = f"""
SELECT event, COUNT(*) as count
FROM sales_fact_part
WHERE event_date = '{sample_date}'
GROUP BY event
"""

start = time.time()
optimized_result = spark.sql(optimized_query)
optimized_result.show()
print(f"Runtime: {time.time() - start:.2f} seconds")

spark.stop()