from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType

def process_trip_zone():

    # read trip_zone data from bronze layer
    trip_zone_df = spark.read.format("csv")\
                        .option("inferSchema", True)\
                        .option("header", True)\
                        .load("abfss://bronze@nyctaxidatalakes.dfs.core.windows.net/trip_zone")

    # Transfrom trip_zone data
    trip_zone_df = trip_zone_df.select(
                F.col("LocationID").cast("int").alias("zone_id"),
                F.trim(F.col("Borough").cast("string")).alias("borough"),
                F.trim(F.col("Zone").cast("string")).alias("zone"),
                F.trim(F.col("service_zone").cast("string")).alias("service_zone")
    )

    trip_zone_df = trip_zone_df.withColumn("zone1", F.get(F.split(F.col("zone"), "/"), 0))\
                            .withColumn("zone2", F.get(F.split(F.col("zone"), "/"), 1))\
                            .drop("zone")

    # write trip_zone data to silver layer
    trip_zone_df.write.format("delta")\
                    .mode("overwrite")\
                    .save("abfss://silver@nyctaxidatalakes.dfs.core.windows.net/trip_zone")

process_trip_zone()