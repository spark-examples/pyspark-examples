from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StructType, StructField, ArrayType, StringType

spark = SparkSession.builder.master("local[1]").appName('Json-Dedup').getOrCreate()

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

json_df = spark.read.format("json").option("multiline", "true").schema(json_schema)\
    .load(['resources/nested_json.json', 'resources/nested_json1.json', 'resources/nested_json2.json'])
parsed_df = json_df.select("Serviceability.CurrentPrinterConfiguration.*", "Serviceability.PrinterData.*").select(
    col('PrinterProductNumber').alias('Product Name'),
    col('PrinterSerialNumber').alias('Serial Number'),
    col('PrinterProductModel').alias('Model Name'),
    col('PrinterFirmwareVersion').alias('Firmware Version'),
    col('MediaDetails').alias('Media'),
    col('SuppliesDetails').alias('Supplies'),
    col('History').alias('history array'))
parsed_df = parsed_df.withColumn('job history', explode(col('history array.val'))).drop(col('history array'))
dedup_df = parsed_df.dropDuplicates(['job history'])
print("Duplicate records dropped : {}".format(parsed_df.count()-dedup_df.count()))
parsed_df.show(truncate=False)
spark.stop()
