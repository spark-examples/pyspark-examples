# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: prabha
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

schema = StructType([
            StructField("input_timestamp", StringType(), True)])

dates = ['2019-07-01 12:01:19.111',
    '2019-06-24 12:01:19.222',
    '2019-11-16 16:44:55.406',
    '2019-11-16 16:50:59.406']

df = spark.createDataFrame(list( zip(dates)), schema=schema)

df.withColumn('input_timestamp',to_timestamp(col('input_timestamp')))\
  .withColumn('current_timestamp', current_timestamp().alias('current_timestamp'))\
  .withColumn('DiffInSeconds',current_timestamp().cast(LongType) - col('input_timestamp').cast(LongType))\
  .withColumn('DiffInMinutes',round(col('DiffInSeconds')/60))\
  .withColumn('DiffInHours',round(col('DiffInSeconds')/3600))\
  .withColumn('DiffInDays',round(col('DiffInSeconds')/24*3600))\
  .show()