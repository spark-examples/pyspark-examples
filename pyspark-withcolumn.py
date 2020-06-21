# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

data = [(("James","","Smith"),"36636","M","3000"), \
      (("Michael","Rose",""),"40288","M","4000"), \
      (("Robert","","Williams"),"42114","M","4000"), \
      (("Maria","Anne","Jones"),"39192","F","4000"), \
      (("Jen","Mary","Brown"),"","F","-1") \
]

schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
          StructField('dob', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', StringType(), True)
         ])


df = spark.createDataFrame(data=data, schema = schema)
df.printSchema()
df.show(truncate=False)

df2 = df.withColumn("salary",col("salary").cast("Integer"))
df2.printSchema()
df2.show(truncate=False)

df3 = df.withColumn("salary",col("salary")*100)
df3.printSchema()
df3.show(truncate=False) 

df4 = df.withColumn("CopiedColumn",col("salary")* -1)
df4.printSchema()

df5 = df.withColumn("Country", lit("USA"))
df5.printSchema()

df6 = df.withColumn("Country", lit("USA")) \
   .withColumn("anotherColumn",lit("anotherValue"))
df6.printSchema()


df.withColumnRenamed("gender","sex") \
  .show(truncate=False) 
  
df4.drop("CopiedColumn") \
.show(truncate=False) 

"""
columns = ["name","address"]
data = [("Robert, Smith", "1 Main st, Newark, NJ, 92537"), \
        ("Maria, Garcia","3456 Walnut st, Newark, NJ, 94732")]

dfFromData = spark.createDataFrame(data=data, schema = schema)

newDF = dfFromData.map(f=>{
nameSplit = f.getAs[String](0).split(",")
addSplit = f.getAs[String](1).split(",")
      (nameSplit(0),nameSplit(1),addSplit(0),addSplit(1),addSplit(2),addSplit(3))
    })
finalDF = newDF.toDF("First Name","Last Name",
             "Address Line1","City","State","zipCode")
finalDF.printSchema()
finalDF.show(false)
"""
