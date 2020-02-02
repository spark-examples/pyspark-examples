# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: prabha
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('changeColNames').getOrCreate()


dataDF = {'animal':['cat','dog','rat'],
          'age':[10,60,50],
          'visits':[1,2,3],
          'priority':['yes','yes','no']
          }

df = spark.createDataFrame(data = [('Bob', 5.62,'juice'), 
                                   ('Sue',0.85,'milk')], schema = ["Name", "Amount","Item"])
df.printSchema()
df.show(truncate=False)
