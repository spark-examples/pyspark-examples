# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""

from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
          .appName('SparkByExamples.com') \
          .getOrCreate()

df=spark.read.option("header",True) \
        .csv("C:/apps/sparkbyexamples/src/pyspark-examples/resources/simple-zipcodes.csv")

          
df.show()
print(df.rdd.getNumPartitions())

df.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("c:/tmp/zipcodes-state")
        
df.write.option("header",True) \
        .partitionBy("state","city") \
        .mode("overwrite") \
        .csv("c:/tmp/zipcodes-state-city")


df=df.repartition(2)

print(df.rdd.getNumPartitions())

df.write.option("header",True) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("c:/tmp/zipcodes-state-more")
        
dfPartition=spark.read.option("header",True)\
                 .csv("c:/tmp/zipcodes-state")
dfPartition.printSchema()

dfSinglePart=spark.read.option("header",True) \
                  .csv("c:/tmp/zipcodes-state/state=AL/city=SPRINGVILLE")
dfSinglePart.printSchema()
dfSinglePart.show()

parqDF = spark.read.option("header",True) \
                  .csv("c:/tmp/zipcodes-state")
parqDF.createOrReplaceTempView("ZIPCODE")
spark.sql("select * from ZIPCODE  where state='AL' and city = 'SPRINGVILLE'") \
    .show()

df.write.option("header",True) \
        .option("maxRecordsPerFile", 2) \
        .partitionBy("state") \
        .mode("overwrite") \
        .csv("/tmp/zipcodes-state-maxrecords")

