# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: Naveen
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, flatten


spark = SparkSession.builder.appName('pyspark-by-examples').getOrCreate()

arrayArrayData = [
  ("James",[["Java","Scala","C++"],["Spark","Java"]]),
  ("Michael",[["Spark","Java","C++"],["Spark","Java"]]),
  ("Robert",[["CSharp","VB"],["Spark","Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema = ['name','subjects'])
df.printSchema()
df.show(truncate=False)

""" """
df.select(df.name,explode(df.subjects)).show(truncate=False)

""" creates a single array from an array of arrays. """
df.select(df.name,flatten(df.subjects)).show(truncate=False)

"""END"""