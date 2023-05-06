# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()

from pyspark.sql.functions import *

df=spark.createDataFrame(
        data = [ ("1","2019-06-24 12:01:19.000")],
        schema=["id","input_timestamp"])
df.printSchema()

#Timestamp String to DateType
df.withColumn("timestamp",to_timestamp("input_timestamp")) \
  .show(truncate=False)
  
# Using Cast to convert TimestampType to DateType
df.withColumn('timestamp', \
         to_timestamp('input_timestamp').cast('string')) \
  .show(truncate=False)
  

df.select(to_timestamp(lit('06-24-2019 12:01:19.000'),'MM-dd-yyyy HH:mm:ss.SSSS')) \
  .show(truncate=False)
  

#SQL string to TimestampType
spark.sql("select to_timestamp('2019-06-24 12:01:19.000') as timestamp")
#SQL CAST timestamp string to TimestampType
spark.sql("select timestamp('2019-06-24 12:01:19.000') as timestamp")
#SQL Custom string to TimestampType
spark.sql("select to_timestamp('06-24-2019 12:01:19.000','MM-dd-yyyy HH:mm:ss.SSSS') as timestamp")