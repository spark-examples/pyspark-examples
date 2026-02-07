"""
PySpark Window Functions - Complete Guide
Author: Sharukh Malik
Description: Comprehensive examples of window functions in PySpark
"""

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F



# Create Spark session
spark = SparkSession.builder.appName("WindowFunctionExample").getOrCreate()

# Sample data
data = [
    ("Sales", "John", 5000, "2024-01"),
    ("Sales", "Jane", 6000, "2024-01"),
    ("Sales", "John", 5500, "2024-02"),
    ("IT", "Bob", 7000, "2024-01"),
    ("IT", "Alice", 7500, "2024-01"),
]


df = spark.createDataFrame(data,["Department","Employee","Salary","Month"])


df.show()


# Example 1: Rank by salary within each department
window_spec = Window.partitionBy("Department").orderBy(F.desc("Salary"))
df_ranked = df.withColumn("Rank", F.rank().over(window_spec))
df_ranked.show()


# Example 2: Running total

window_running = Window.partitionBy("Department").orderBy("month").rowsBetween(Window.unboundedPreceding,Window.currentRow)
df_running = df.withColumn("Running_Total",F.sum("Salary").over(window_running))
df_running.show()


# Example 3: Lead and Lag
window_order = Window.partitionBy("Employee").orderBy("Month")
df_lead_lag = df.withColumn("Previous_Salary", F.lag("Salary", 1).over(window_order)) \
                .withColumn("Next_Salary", F.lead("Salary", 1).over(window_order))
df_lead_lag.show()


# Example 4: Percent rank
df_percent = df.withColumn("Percent_Rank", F.percent_rank().over(window_spec))
df_percent.show()