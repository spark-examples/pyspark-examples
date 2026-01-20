# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

from pyspark.sql.types import ArrayType,IntegerType
arrayType = ArrayType(IntegerType(),False)
print(arrayType.jsonValue()) 
print(arrayType.simpleString())
print(arrayType.typeName()) 


from pyspark.sql.types import MapType,StringType,IntegerType
mapType = MapType(StringType(),IntegerType())
 
print(mapType.keyType)
print(mapType.valueType)
print(mapType.valueContainsNull)

data = [("James","","Smith","36","M",3000),
    ("Michael","Rose","","40","M",4000),
    ("Robert","","Williams","42","M",4000),
    ("Maria","Anne","Jones","39","F",4000),
    ("Jen","Mary","Brown","","F",-1)
  ]

schema = StructType([ 
    StructField("firstname",StringType(),True), 
    StructField("middlename",StringType(),True), 
    StructField("lastname",StringType(),True), 
    StructField("age", StringType(), True), 
    StructField("gender", StringType(), True), 
    StructField("salary", IntegerType(), True) 
  ])


df = spark.createDataFrame(data=data,schema=schema)
df.printSchema()
df.show(truncate=False)









