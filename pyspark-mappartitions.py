# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = [('James','Smith','M',3000),
  ('Anna','Rose','F',4100),
  ('Robert','Williams','M',6200), 
]

columns = ["firstname","lastname","gender","salary"]
df = spark.createDataFrame(data=data, schema = columns)
df.show()

#Example 1 mapPartitions()
def reformat(partitionData):
    for row in partitionData:
        yield [row.firstname+","+row.lastname,row.salary*10/100]
df.rdd.mapPartitions(reformat).toDF().show()

#Example 2 mapPartitions()
def reformat2(partitionData):
  updatedData = []
  for row in partitionData:
    name=row.firstname+","+row.lastname
    bonus=row.salary*10/100
    updatedData.append([name,bonus])
  return iter(updatedData)

df2=df.rdd.mapPartitions(reformat2).toDF("name","bonus")
df2.show()




