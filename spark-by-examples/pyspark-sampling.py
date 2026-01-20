# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

df=spark.range(100)

'''sample() '''
print(df.sample(0.06).collect())
print(df.sample(0.1,123).collect())
print(df.sample(0.1,123).collect())
print(df.sample(0.1,456).collect())
print("withReplacement Examples")
print(df.sample(True,0.3,123).collect())
print(df.sample(0.3,123).collect())

'''sampleBy() '''
print("sampleBy Examples")
df2=df.select((df.id % 3).alias("key"))
print(df2.sampleBy("key", {0: 0.1, 1: 0.2},0).collect())


print("RDD Examples")
'''RDD'''
rdd = spark.sparkContext.range(0,100)
print(rdd.sample(False,0.1,0).collect())
print(rdd.sample(True,0.3,123).collect())

''' RDD takeSample() '''
print(rdd.takeSample(False,10,0))
print(rdd.takeSample(True,30,123))
