from pyspark.sql import functions as F
from delta.tables import DeltaTable

SOURCE_PATH = "abfss://silver@nyctaxidatalakes.dfs.core.windows.net/trip_payment"
TARGET_PATH = "abfss://gold@nyctaxidatalakes.dfs.core.windows.net/dim_trip_payment"

def create_dim_payment_table():
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.dim_trip_payment(
            payment_type_sk LONG,
            payment_type_id INTEGER,
            payment_type STRING
        )
        USING DELTA LOCATION '{TARGET_PATH}'
    """)
 
def process_dim_payment():
    create_dim_payment_table()

    payment_df = spark.read.format("delta") \
        .option("path", SOURCE_PATH) \
        .load() \
        .dropDuplicates(["payment_type_id"]) \
        .withColumns({"payment_type_sk": F.monotonically_increasing_id() + 1})

    dim_payment = DeltaTable.forPath(spark, TARGET_PATH)

    dim_payment.alias("target").merge(
        payment_df.alias("source"),
        "target.payment_type_id = source.payment_type_id"
    ).whenMatchedUpdate(
        set={
            "payment_type_id": "source.payment_type_id",
            "payment_type": "source.payment_type"
        }
    ).whenNotMatchedInsertAll().execute()

process_dim_payment()