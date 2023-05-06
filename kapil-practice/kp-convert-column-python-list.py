from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()

nasa_df = spark.read.json("G:\\datasets\\input-datasets\\json-datasets\\nasa-singleline-zipcodes.json")
nasa_df.printSchema()

nasa_df.show()

# all_cities = nasa_df.select("City").distinct().rdd.map(lambda x: x.City).collect()
# print(all_cities)
spark.stop()