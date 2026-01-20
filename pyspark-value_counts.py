from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

# Create a SparkSession
spark = SparkSession.builder.appName("ValueCountsExample").getOrCreate()

# Create a PySpark DataFrame
data = [("apple", 5), ("orange", 3), ("banana", 2), ("apple", 3), ("orange", 4)]
df = spark.createDataFrame(data, schema=["fruit", "quantity"])

# Use groupBy and count to get the counts for each distinct value in the 'fruit' column
counts = df.groupBy("fruit").count().orderBy(desc("count"))

# Show the results
counts.show()

# Stop the SparkSession
spark.stop()
