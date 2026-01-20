from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
from pyspark.sql.types import *

spark = SparkSession.builder.\
master("local").\
appName("prac").\
getOrCreate()

sc = spark.sparkContext
sc._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
sc._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

sc._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'A')
sc._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'VracelDcLui')
sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.amazonaws.com")

df = spark.read.option("inferSchema","true").option("header","true").csv("s3a://ind-north-up-agra-kapil-test-1/datasets/input-ds/movies/")
df.show()

spark.stop()