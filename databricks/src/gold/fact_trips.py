from pyspark.sql import functions as F
import sys 

SOURCE_PATH = "abfss://silver@nyctaxidatalakes.dfs.core.windows.net/green_taxi"
TARGET_PATH = "abfss://gold@nyctaxidatalakes.dfs.core.windows.net/fact_trips"

def get_pipeline_params():
    p_year = int(sys.argv[1])
    p_month = int(sys.argv[2])
    return p_year, p_month

def create_fact_trips_table():
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.fact_trips (
            trip_sk LONG,
            pickup_zone_sk LONG,
            dropoff_zone_sk LONG,
            trip_type_sk LONG,
            payment_type_sk LONG,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INT,
            trip_distance DOUBLE,
            fare_amount DOUBLE,
            total_amount DOUBLE,
            extra_charge DOUBLE,
            trip_duration DOUBLE,
            average_speed DOUBLE,
            year INT,
            month INT
        )
        USING DELTA
        LOCATION '{TARGET_PATH}'
        PARTITIONED BY (year, month)
    """)


def process_fact_trips():
    p_year, p_month = get_pipeline_params()
    
    # Read dimesion tables in gold layer
    dim_zone = spark.read.table("gold.dim_trip_zone")
    dim_type = spark.read.table("gold.dim_trip_type")
    dim_payment = spark.read.table("gold.dim_trip_payment")
    
    # read newly trip data from silver layer
    silver_df = spark.read.format("delta") \
        .load(SOURCE_PATH) \
        .filter((F.col("year") == p_year) & (F.col("month") == p_month))
        
    # Join silver data with dimesion tables
    fact_df = silver_df \
        .join(dim_zone.alias("pu"), silver_df.pickup_zone_id == F.col("pu.zone_id"), "left") \
        .join(dim_zone.alias("do"), silver_df.dropoff_zone_id == F.col("do.zone_id"), "left") \
        .join(dim_type, silver_df.trip_type_id == dim_type.trip_type_id, "left") \
        .join(dim_payment, silver_df.payment_id == dim_payment.payment_type_id, "left") \
        .select(
            F.monotonically_increasing_id().alias("trip_sk"),
            F.col("pu.zone_sk").alias("pickup_zone_sk"),
            F.col("do.zone_sk").alias("dropoff_zone_sk"),
            dim_type.trip_type_sk,
            dim_payment.payment_type_sk,
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "extra_charge",
            "trip_duration",
            "average_speed",
            "year",
            "month"
        )
        
    # Write Dynamic Partition Overwrite
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    fact_df.write.format("delta") \
        .mode("overwrite") \
        .option("path", TARGET_PATH)\
        .partitionBy("year", "month") \
        .save()

create_fact_trips_table()
process_fact_trips()