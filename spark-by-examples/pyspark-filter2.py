# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data2 = [(1,"James Smith"), (2,"Michael Rose"),
    (3,"Robert Williams"), (4,"Rames Rose"),(5,"Rames rose")
  ]
df2 = spark.createDataFrame(data = data2, schema = ["id","name"])

df2.filter(df2.name.like("%rose%")).show()
df2.filter(df2.name.rlike("(?i)^*rose$")).show()