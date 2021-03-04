# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
               .appName('SparkByExamples.com') \
               .getOrCreate()

from pyspark.sql.functions import *

df=spark.createDataFrame([["02-03-2013"],["05-06-2023"]],["input"])
df.select(col("input"),to_date(col("input"),"MM-dd-yyyy").alias("date")) \
  .show()

#SQL
spark.sql("select to_date('02-03-2013','MM-dd-yyyy') date").show()
  

