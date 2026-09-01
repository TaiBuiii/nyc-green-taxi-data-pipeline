from pyspark.sql import DataFrame, functions as F
import sys 

def get_pipeline_params() -> tuple[int, int]:
    p_year = int(sys.argv[1])
    p_month = int(sys.argv[2])
    return p_year, p_month

def read_bronze_data(year: int, month: int) -> DataFrame:
    return spark.read.format("parquet") \
        .load("abfss://bronze@nyctaxidatalakes.dfs.core.windows.net/green_taxi") \
        .filter((F.col("year") == year) & (F.col("month") == month))

def cast_data_type(df: DataFrame):
    return df.select(
        F.col("lpep_pickup_datetime").cast("timestamp").alias("pickup_datetime"),
        F.col("lpep_dropoff_datetime").cast("timestamp").alias("dropoff_datetime"),
        F.col("PULocationID").cast("integer").alias("pickup_zone_id"),
        F.col("DOLocationID").cast("integer").alias("dropoff_zone_id"),
        F.col("payment_type").cast("integer").alias("payment_id"),
        F.col("trip_type").cast("integer") .alias("trip_type_id"),
        F.col("passenger_count").cast("integer").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("year").cast("integer").alias("year"),
        F.col("month").cast("integer").alias("month")
    )
def clean_data(df: DataFrame) -> DataFrame:
    return df.filter(
        (F.col("pickup_datetime").isNotNull()) &
        (F.col("dropoff_datetime").isNotNull()) &
        (F.col("pickup_zone_id").isNotNull()) &
        (F.col("dropoff_zone_id").isNotNull()) &
        (F.col("payment_id").isNotNull()) &
        (F.col("trip_type_id").isNotNull()) &
        (F.col("passenger_count") > 0) &
        (F.col("trip_distance") > 0) &
        (F.col("fare_amount") > 0) &
        (F.col("total_amount") > 0) &
        (F.col("dropoff_datetime") > F.col("pickup_datetime")) 
    )

def transform_features(df: DataFrame) -> DataFrame:
    return df \
        .withColumn("trip_distance", F.round(F.col("trip_distance") * 1.60934, 2)) \
        .withColumn("trip_duration", F.round((F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60, 2)) \
        .withColumn("average_speed", F.round(F.col("trip_distance") / (F.col("trip_duration") / 60), 2)) \
        .withColumn("extra_charge", F.round(F.col("total_amount") - F.col("fare_amount"), 2)) 

def handle_outliers(df: DataFrame, numerical_attrs: list[str]) -> DataFrame:
    def filter_outlier(data_df: DataFrame, attr: str) -> DataFrame:
        quantiles = data_df.stat.approxQuantile(attr, [0.25, 0.75], 0.01)
        Q1, Q3 = quantiles[0], quantiles[1]
        IQR = Q3 - Q1
        LF, UF = Q1 - 1.5 * IQR, Q3 + 1.5 * IQR
        return data_df.filter((F.col(attr) <= UF) & (F.col(attr) >= LF))

    for attr in numerical_attrs:
        df = filter_outlier(df, attr)
    return df

def write_silver_data(df: DataFrame) -> None:
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.format("delta") \
        .mode("overwrite") \
        .partitionBy("year", "month") \
        .save("abfss://silver@nyctaxidatalakes.dfs.core.windows.net/green_taxi")

def run_pipeline() -> None:
    p_year, p_month = get_pipeline_params()
    
    numerical_attrs = [
        "trip_distance", "trip_duration", "average_speed"
    ]
    
    # Executing functional flow
    df_raw = read_bronze_data(p_year, p_month)
    df_casted = cast_data_type(df_raw)
    df_cleaned = clean_data(df_casted)
    df_transformed = transform_features(df_cleaned)
    df_final = handle_outliers(df_transformed, numerical_attrs)
    
    write_silver_data(df_final)

run_pipeline()