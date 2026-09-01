from pyspark.sql import functions as F
from delta.tables import DeltaTable

SOURCE_PATH = "abfss://silver@nyctaxidatalakes.dfs.core.windows.net/trip_type"
TARGET_PATH = "abfss://gold@nyctaxidatalakes.dfs.core.windows.net/dim_trip_type"

def create_dim_type_table():
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.dim_trip_type (
            trip_type_sk LONG,
            trip_type_id INTEGER,
            trip_type STRING
        )
        USING DELTA LOCATION '{TARGET_PATH}'
    """)

def process_dim_type():
    create_dim_type_table()

    trip_type_df = spark.read.format("delta")\
        .option("path", SOURCE_PATH)\
        .load()\
        .dropDuplicates(["trip_type_id"]) \
        .withColumn("trip_type_sk", F.monotonically_increasing_id() + 1)

    dim_trip_type = DeltaTable.forPath(spark, TARGET_PATH)
    
    dim_trip_type.alias("target").merge(
        trip_type_df.alias("source"),
        "target.trip_type_id = source.trip_type_id"
    ).whenMatchedUpdate(
        set = {
            "trip_type_id": "source.trip_type_id",
            "trip_type": "source.trip_type"
        }
    ).whenNotMatchedInsertAll().execute()


process_dim_type()