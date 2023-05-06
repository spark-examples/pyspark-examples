# -*- coding: utf-8 -*-
"""
Created on Sat Jun 13 21:08:30 2020

@author: NNK
"""

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [("James","Smith","USA","CA"),
    ("Michael","Rose","USA","NY"),
    ("Robert","Williams","USA","CA"),
    ("Maria","Jones","USA","FL")
  ]

columns = ["firstname","lastname","country","state"]
df = spark.createDataFrame(data = data, schema = columns)
df.show(truncate=False)


df.select("firstname","lastname").show()

#Using Dataframe object name
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()

# Using col function
from pyspark.sql.functions import col
df.select(col("firstname").alias("fname"),col("lastname")).show()

# Show all columns
df.select("*").show()
df.select([col for col in df.columns]).show()
df.select(*columns).show()

df.select(df.columns[:3]).show(3)
df.select(df.columns[2:4]).show(3)

df.select(df.colRegex("`^.*name*`")).show()

data = [
        (("James",None,"Smith"),"OH","M"),
        (("Anna","Rose",""),"NY","F"),
        (("Julia","","Williams"),"OH","F"),
        (("Maria","Anne","Jones"),"NY","M"),
        (("Jen","Mary","Brown"),"NY","M"),
        (("Mike","Mary","Williams"),"OH","M")
        ]

from pyspark.sql.types import StructType,StructField, StringType        
schema = StructType([
    StructField('name', StructType([
         StructField('firstname', StringType(), True),
         StructField('middlename', StringType(), True),
         StructField('lastname', StringType(), True)
         ])),
     StructField('state', StringType(), True),
     StructField('gender', StringType(), True)
     ])


df2 = spark.createDataFrame(data = data, schema = schema)
df2.printSchema()
df2.show(truncate=False) # shows all columns
df2.select("name").show(truncate=False)
df2.select("name.firstname","name.lastname").show(truncate=False)
df2.select("name.*").show(truncate=False)


