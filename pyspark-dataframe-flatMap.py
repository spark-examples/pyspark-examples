# -*- coding: utf-8 -*-
"""
author SparkByExamples.com
"""


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

columns = ["name","languagesAtSchool","currentState"]
data = [("James,,Smith",["Java","Scala","C++"],"CA"), \
    ("Michael,Rose,",["Spark","Java","C++"],"NJ"), \
    ("Robert,,Williams",["CSharp","VB"],"NV")]

# Convert data to a DataFrame
rdd = spark.sparkContext.parallelize(data)
row_rdd = rdd.map(lambda x: Row(name=x[0], languagesAtSchool=x[1], currentState=x[2]))
df = spark.createDataFrame(row_rdd, columns)

# Apply flatMap transformation
flat_mapped_df = df.rdd.flatMap(lambda x: [(x["name"], lang, x["currentState"]) for lang in x["languagesAtSchool"]])

# Convert result to DataFrame
result_columns = ["name", "language", "currentState"]
result_df = flat_mapped_df.toDF(result_columns)

# Show the result
result_df.show()  

