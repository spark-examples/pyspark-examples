# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType
# Create SparkSession
spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()

simpleData = [("James","34","true","M","3000.6089"),
    ("Michael","33","true","F","3300.8067"),
    ("Robert","37","false","M","5000.5034")
  ]

columns = ["firstname","age","isGraduated","gender","salary"]
df = spark.createDataFrame(data = simpleData, schema = columns)
df.printSchema()
df.show(truncate=False)

from pyspark.sql.functions import col,round,expr
df.withColumn("salary",df.salary.cast('double')).printSchema()    
df.withColumn("salary",df.salary.cast(DoublerType())).printSchema()    
df.withColumn("salary",col("salary").cast('double')).printSchema()    

#df.withColumn("salary",round(df.salary.cast(DoubleType()),2)).show(truncate=False).printSchema()    
df.selectExpr("firstname","isGraduated","cast(salary as double) salary").printSchema()    

df.createOrReplaceTempView("CastExample")
spark.sql("SELECT firstname,isGraduated,DOUBLE(salary) as salary from CastExample").printSchema()


#df.select("firstname",expr(df.age),"isGraduated",col("salary").cast('float').alias("salary")).show()
