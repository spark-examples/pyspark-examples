# -*- coding: utf-8 -*-
"""
Created on Sun Jun 14 10:20:19 2020
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()
data = ["Project Gutenberg’s",
        "Alice’s Adventures in Wonderland",
        "Project Gutenberg’s",
        "Adventures in Wonderland",
        "Project Gutenberg’s"]
rdd=spark.sparkContext.parallelize(data)

for element in rdd.collect():
    print(element)

#Flatmap    
rdd2=rdd.flatMap(lambda x: x.split(" "))
for element in rdd2.collect():
    print(element)
#map
rdd3=rdd2.map(lambda x: (x,1))
for element in rdd3.collect():
    print(element)
#reduceByKey
rdd4=rdd3.reduceByKey(lambda a,b: a+b)
for element in rdd4.collect():
    print(element)
#map
rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
for element in rdd5.collect():
    print(element)
#filter
rdd6 = rdd5.filter(lambda x : 'a' in x[1])
for element in rdd6.collect():
    print(element)

from pyspark.sql.functions import col,expr
data=[("2019-01-23",1),("2019-06-24",2),("2019-09-20",3)]
spark.createDataFrame(data).toDF("date","increment") \
    .select(col("date"),col("increment"), \
      expr("add_months(to_date(date,'yyyy-MM-dd'),cast(increment as int))").alias("inc_date")) \
    .show()