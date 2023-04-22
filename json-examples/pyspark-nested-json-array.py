# -*- coding: utf-8 -*-
"""
author Ayush Agarwal : https://github.com/ayush-96
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

spark = SparkSession.builder.master("local[1]").appName("Nested JSON Reader").getOrCreate()

# Define the schema for the JSON data
json_schema = StructType([
    StructField("Serviceability", StructType([
        StructField("CurrentPrinterConfiguration", StructType([
            StructField("PrinterProductNumber", StringType()),
            StructField("PrinterSerialNumber", StringType()),
            StructField("PrinterProductModel", StringType()),
            StructField("PrinterFirmwareVersion", StringType())
        ])),
        StructField("PrinterData", StructType([
            StructField("MediaDetails", StringType()),
            StructField("SuppliesDetails", StringType()),
            StructField("History", ArrayType(StructType([
                StructField("val", StringType())
            ])))
        ]))
    ]))
])

# Read the JSON file into a DataFrame
df = spark.read.format("json").option("multiline", "true").schema(json_schema).load("../resources/nested_json.json")
parsed_df = df.select("Serviceability.CurrentPrinterConfiguration.*", "Serviceability.PrinterData.*").select(
    col('PrinterProductNumber').alias('Product Name'),
    col('PrinterSerialNumber').alias('Serial Number'),
    col('PrinterProductModel').alias('Model Name'),
    col('PrinterFirmwareVersion').alias('Firmware Version'),
    col('MediaDetails').alias('Media'),
    col('SuppliesDetails').alias('Supplies'),
    col('History').alias('history array'))
parsed_df = parsed_df.withColumn('job history', explode(col('history array'))).drop(col('history array'))
parsed_df.show(truncate=False)
