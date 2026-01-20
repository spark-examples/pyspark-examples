# -*- coding: utf-8 -*-
'''
Created on Sat Jan 11 19:38:27 2020

@author: sparkbyexamples.com
'''

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.functions import *


spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


dataDF = [(('James','','Smith'),'1991-04-01','M',3000),
  (('Michael','Rose',''),'2000-05-19','M',4000),
  (('Robert','','Williams'),'1978-09-05','M',4000),
  (('Maria','Anne','Jones'),'1967-12-01','F',4000),
  (('Jen','Mary','Brown'),'1980-02-17','F',-1)
]

schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)
         ])

df = spark.createDataFrame(data = dataDF, schema = schema)
df.printSchema()

''' Example 1 '''
df.withColumnRenamed("dob","DateOfBirth").printSchema()
''' Example 2 '''    
df2 = df.withColumnRenamed("dob","DateOfBirth") \
    .withColumnRenamed("salary","salary_amount")
df2.printSchema()

''' Example 3 '''
schema2 = StructType([
    StructField("fname",StringType()),
    StructField("middlename",StringType()),
    StructField("lname",StringType())])
    
df.select(col("name").cast(schema2),
  col("dob"),
  col("gender"),
  col("salary")) \
    .printSchema()    

''' Example 4 '''
df.select(col("name.firstname").alias("fname"),
  col("name.middlename").alias("mname"),
  col("name.lastname").alias("lname"),
  col("dob"),col("gender"),col("salary")) \
  .printSchema()
  
''' Example 5 '''  
df4 = df.withColumn("fname",col("name.firstname")) \
      .withColumn("mname",col("name.middlename")) \
      .withColumn("lname",col("name.lastname")) \
      .drop("name")
df4.printSchema()

''' Example 6

not working
val old_columns = Seq("dob","gender","salary","fname","mname","lname")
    val new_columns = Seq("DateOfBirth","Sex","salary","firstName","middleName","lastName")
    val columnsList = old_columns.zip(new_columns).map(f=&gt;{col(f._1).as(f._2)})
    val df5 = df4.select(columnsList:_*)
    df5.printSchema()
'''
''' Example 7
not working 
newColumns = ["newCol1","newCol2","newCol3","newCol4"]
df.toDF(newColumns) \
.printSchema()
'''        