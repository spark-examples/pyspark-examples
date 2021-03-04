# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()


dates = [("1","2019-07-01 12:01:19.111"),
    ("2","2019-06-24 12:01:19.222"),
    ("3","2019-11-16 16:44:55.406"),
    ("4","2019-11-16 16:50:59.406")
    ]

df = spark.createDataFrame(data=dates, schema=["id","from_timestamp"])

from pyspark.sql.functions import *
df2=df.withColumn('from_timestamp',to_timestamp(col('from_timestamp')))\
  .withColumn('end_timestamp', current_timestamp())\
  .withColumn('DiffInSeconds',col("end_timestamp").cast("long") - col('from_timestamp').cast("long"))
df2.show(truncate=False)

df.withColumn('from_timestamp',to_timestamp(col('from_timestamp')))\
  .withColumn('end_timestamp', current_timestamp())\
  .withColumn('DiffInSeconds',unix_timestamp("end_timestamp") - unix_timestamp('from_timestamp')) \
  .show(truncate=False)

df2.withColumn('DiffInMinutes',round(col('DiffInSeconds')/60))\
  .show(truncate=False)
  
df2.withColumn('DiffInHours',round(col('DiffInSeconds')/3600))\
  .show(truncate=False)
  
#Difference between two timestamps when input has just timestamp

data= [("12:01:19.000","13:01:19.000"),
    ("12:01:19.000","12:02:19.000"),
    ("16:44:55.406","17:44:55.406"),
    ("16:50:59.406","16:44:59.406")]
df3 = spark.createDataFrame(data=data, schema=["from_timestamp","to_timestamp"])

df3.withColumn("from_timestamp",to_timestamp(col("from_timestamp"),"HH:mm:ss.SSS")) \
   .withColumn("to_timestamp",to_timestamp(col("to_timestamp"),"HH:mm:ss.SSS")) \
   .withColumn("DiffInSeconds", col("from_timestamp").cast("long") - col("to_timestamp").cast("long")) \
   .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60)) \
   .withColumn("DiffInHours",round(col("DiffInSeconds")/3600)) \
   .show(truncate=False)
   
#


df3 = spark.createDataFrame(
        data=[("1","07-01-2019 12:01:19.406")], 
        schema=["id","input_timestamp"]
        )
df3.withColumn("input_timestamp",to_timestamp(col("input_timestamp"),"MM-dd-yyyy HH:mm:ss.SSS")) \
    .withColumn("current_timestamp",current_timestamp().alias("current_timestamp")) \
    .withColumn("DiffInSeconds",current_timestamp().cast("long") - col("input_timestamp").cast("long")) \
    .withColumn("DiffInMinutes",round(col("DiffInSeconds")/60)) \
    .withColumn("DiffInHours",round(col("DiffInSeconds")/3600)) \
    .withColumn("DiffInDays",round(col("DiffInSeconds")/24*3600)) \
    .show(truncate=False)
    
#SQL

spark.sql("select unix_timestamp('2019-07-02 12:01:19') - unix_timestamp('2019-07-01 12:01:19') DiffInSeconds").show()
spark.sql("select (unix_timestamp('2019-07-02 12:01:19') - unix_timestamp('2019-07-01 12:01:19'))/60 DiffInMinutes").show()
spark.sql("select (unix_timestamp('2019-07-02 12:01:19') - unix_timestamp('2019-07-01 12:01:19'))/3600 DiffInHours").show()