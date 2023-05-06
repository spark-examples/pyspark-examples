# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col
spark=SparkSession.builder.appName("sparkbyexamples").getOrCreate()

data=data = [('James','','Smith','1991-04-01'),
  ('Michael','Rose','','2000-05-19'),
  ('Robert','','Williams','1978-09-05'),
  ('Maria','Anne','Jones','1967-12-01'),
  ('Jen','Mary','Brown','1980-02-17')
]

columns=["firstname","middlename","lastname","dob"]
df=spark.createDataFrame(data,columns)
df.printSchema()
df.show(truncate=False)
df1 = df.withColumn('year', split(df['dob'], '-').getItem(0)) \
       .withColumn('month', split(df['dob'], '-').getItem(1)) \
       .withColumn('day', split(df['dob'], '-').getItem(2))
df1.printSchema()
df1.show(truncate=False)

 # Alternatively we can do like below      
split_col = pyspark.sql.functions.split(df['dob'], '-')
df2 = df.withColumn('year', split_col.getItem(0)) \
       .withColumn('month', split_col.getItem(1)) \
       .withColumn('day', split_col.getItem(2))
df2.show(truncate=False)      

# Using split() function of Column class
split_col = pyspark.sql.functions.split(df['dob'], '-')
df3 = df.select("firstname","middlename","lastname","dob", split_col.getItem(0).alias('year'),split_col.getItem(1).alias('month'),split_col.getItem(2).alias('day'))   
df3.show(truncate=False)

"""
df4=spark.createDataFrame([("20-13-2012-monday",)], ['date',])

df4.select(split(df4.date,'^([\d]+-[\d]+-[\d])').alias('date'),
    regexp_replace(split(df4.date,'^([\d]+-[\d]+-[\d]+)').getItem(1),'-','').alias('day')).show()
    """
df4 = spark.createDataFrame([('oneAtwoBthree',)], ['str',])
df4.select(split(df4.str, '[AB]').alias('str')).show()

df4.select(split(df4.str, '[AB]',2).alias('str')).show()
df4.select(split(df4.str, '[AB]',1).alias('str')).show()