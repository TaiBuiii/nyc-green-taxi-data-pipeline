from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, LongType, DoubleType, StringType, TimestampType


def process_trip_payemnt():
    # read trip_payement data from bronze
    trip_payment_df = spark.read.format("csv")\
                        .option("inferSchema", True)\
                        .option("header", True)\
                        .load("abfss://bronze@nyctaxidatalakes.dfs.core.windows.net/trip_payment")

    # transform trip_payemnt data
    trip_payment_df = trip_payment_df.select(
                F.col("payment_type_code").cast("int").alias("payment_type_id"),
                F.trim(F.col("payment_type").cast("string")).alias("payment_type")
    )

    # write trip_payment data to silver
    trip_payment_df.write.format("delta")\
                    .mode("overwrite")\
                    .save("abfss://silver@nyctaxidatalakes.dfs.core.windows.net/trip_payment")          

process_trip_payemnt()