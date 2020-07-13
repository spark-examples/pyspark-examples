# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import DataType
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

arrayType = ArrayType(IntegerType(),False)
print(arrayType.jsonValue)  

print(arrayType.simpleString)

print(arrayType.typeName)

