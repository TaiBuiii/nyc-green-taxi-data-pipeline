from pyspark.sql import functions as F
from delta.tables import DeltaTable

SOURCE_PATH = "abfss://silver@nyctaxidatalakes.dfs.core.windows.net/trip_zone"
TARGET_PATH = "abfss://gold@nyctaxidatalakes.dfs.core.windows.net/dim_trip_zone"

def create_dim_zone_table():
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.dim_trip_zone (
            zone_sk LONG,
            zone_id INTEGER,
            borough STRING,
            service_zone STRING,
            zone1 STRING,
            zone2 STRING
        )
        USING DELTA LOCATION '{TARGET_PATH}'
    """)

def process_dim_zone():
    create_dim_zone_table()

    zone_df = spark.read.format("delta") \
        .option("path", SOURCE_PATH) \
        .load() \
        .dropDuplicates(["zone_id"]) \
        .withColumn("zone_sk", F.monotonically_increasing_id() + 1)

    dim_zone = DeltaTable.forName(spark, "gold.dim_trip_zone")

    dim_zone.alias("target").merge(
        zone_df.alias("source"),
        "target.zone_id = source.zone_id"
    ).whenMatchedUpdate(
        set = {
            "borough": "source.borough",
            "service_zone": "source.service_zone",
            "zone1": "source.zone1",
            "zone2": "source.zone2"
        }
    ).whenNotMatchedInsertAll().execute()

process_dim_zone()