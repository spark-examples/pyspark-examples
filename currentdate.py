# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: prabha

Modified on Wed Feb 21 2024
@author: Shahid

"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import  DateType
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

# Sample data with a string column representing dates
data = [("1/12/2023",), ("2/15/2023",), ("3/20/2023",)]
columns = ["date_str"]

df = spark.createDataFrame(data, columns)

# Convert the string column to a DateType
df = df.withColumn("date", to_date("date_str", "M/d/yyyy").cast(DateType()))

df.show()
