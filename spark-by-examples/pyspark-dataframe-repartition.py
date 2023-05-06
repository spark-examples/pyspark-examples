# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
        .master("local[5]").getOrCreate()

df=spark.range(0,20)
print(df.rdd.getNumPartitions())

df.write.mode("overwrite").csv("c:/tmp/partition.csv")

df2 = df.repartition(6)
print(df2.rdd.getNumPartitions())

df3 = df.coalesce(2)
print(df3.rdd.getNumPartitions())

df4 = df.groupBy("id").count()
print(df4.rdd.getNumPartitions())