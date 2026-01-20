# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020
"""
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

print(spark)

