# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: prabha
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

schema = StructType([
            StructField("city", StringType(), True),
            StructField("dates", StringType(), True),
            StructField("population", IntegerType(), True)])

dates = ["1991-02-25","1998-05-10", "1993/03/15", "1992/07/17"]
cities = ['Caracas', 'Ccs', '   São Paulo   ', '~Madrid']
population = [37800000, 19795791, 12341418, 6489162]

        # Dataframe:
df = spark.createDataFrame(list(zip(cities, dates, population)), schema=schema)

df.show(truncate=False)