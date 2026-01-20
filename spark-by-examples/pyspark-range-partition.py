# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()
          

data = [(1,10),(2,20),(3,10),(4,20),(5,10),
    (6,30),(7,50),(8,50),(9,50),(10,30),
    (11,10),(12,10),(13,40),(14,40),(15,40),
    (16,40),(17,50),(18,10),(19,40),(20,40)
  ]

df=spark.createDataFrame(data,["id","value"])

df.repartition(3,"value").explain(True)        
df.repartition("value") \
  .write.option("header",True) \
  .mode("overwrite") \
  .csv("c:/tmp/range-partition")

df.repartitionByRange("value").explain(True)
df.repartitionByRange(3,"value").explain(True)

df.repartitionByRange(3,"value") \
  .write.option("header",True) \
  .mode("overwrite") \
  .csv("c:/tmp/range-partition-count")

