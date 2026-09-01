from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType

def process_trip_type():
    # read trip_type data from bronze layer
    df = spark.read.format("csv") \
        .option("inferSchema", True) \
        .option("header", True) \
        .load("abfss://bronze@nyctaxidatalakes.dfs.core.windows.net/trip_type")

    # transform trip_type data
    df = df.select(
        F.col("trip_type").cast("int").alias("trip_type_id"),
        F.trim(F.col("description").cast("string")).alias("trip_type")
    )

    # write data to silver layer
    df.write.format("delta") \
        .mode("overwrite") \
        .save("abfss://silver@nyctaxidatalakes.dfs.core.windows.net/trip_type")

process_trip_type()