import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def main():
    # ---------------------------------------------------------
    # 1. Initialize Spark Session with Controlled Concurrency
    # ---------------------------------------------------------
    # Using local[2] and 8 shuffle partitions prevents WSL OOM crashes
    # caused by keeping 1,000+ partition writers open simultaneously.
    spark = (
        SparkSession.builder
        .appName("NYCTaxi-Distributed-Analytics")
        .master("local[2]")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.files.maxPartitionBytes", "67108864") # 64MB partition chunks
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_trip_path = "data/raw/yellow_tripdata"
    raw_zone_path = "data/raw/zone_lookup/taxi_zone_lookup.csv"
    processed_base_path = "data/processed"

    print(">>> [1/7] Reading raw trip data as single partitioned DataFrame...")
    # Passing the directory directly avoids the Hadoop wildcard FileStreamSink warning
    df_raw = spark.read.parquet(raw_trip_path)
    total_raw_rows = df_raw.count()
    print(f"    Raw records ingested: {total_raw_rows:,}")

    # ---------------------------------------------------------
    # 2. Column Pruning
    # ---------------------------------------------------------
    columns_to_keep = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "trip_distance",
        "passenger_count",
        "fare_amount",
        "total_amount"
    ]
    df_pruned = df_raw.select(*columns_to_keep)

    # ---------------------------------------------------------
    # 3. Read Dimension Table
    # ---------------------------------------------------------
    print(">>> [2/7] Loading and preparing taxi zone dimension table...")
    df_zones = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(raw_zone_path)
        .select(
            F.col("LocationID"),
            F.col("Borough"),
            F.col("Zone"),
            F.col("service_zone")
        )
    )

    # ---------------------------------------------------------
    # 4. Null Audits & Explicit Broadcast Joins
    # ---------------------------------------------------------
    print(">>> [3/7] Performing explicit broadcast joins...")
    null_pu_before = df_pruned.filter(F.col("PULocationID").isNull()).count()
    null_do_before = df_pruned.filter(F.col("DOLocationID").isNull()).count()

    pu_zones = (
        df_zones.select(
            F.col("LocationID").alias("pu_loc_id"),
            F.col("Borough").alias("pickup_borough"),
            F.col("Zone").alias("pickup_zone"),
            F.col("service_zone").alias("pickup_service_zone")
        )
    )

    do_zones = (
        df_zones.select(
            F.col("LocationID").alias("do_loc_id"),
            F.col("Borough").alias("dropoff_borough"),
            F.col("Zone").alias("dropoff_zone"),
            F.col("service_zone").alias("dropoff_service_zone")
        )
    )

    df_joined = (
        df_pruned
        .join(F.broadcast(pu_zones), df_pruned.PULocationID == pu_zones.pu_loc_id, "left")
        .join(F.broadcast(do_zones), df_pruned.DOLocationID == do_zones.do_loc_id, "left")
        .drop("pu_loc_id", "do_loc_id")
    )

    # ---------------------------------------------------------
    # 5. Handling Unknown Zones & Filtering Outliers
    # ---------------------------------------------------------
    df_enriched = (
        df_joined
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
        .withColumn("pickup_hour", F.hour(F.col("tpep_pickup_datetime")))
        .withColumn(
            "pickup_borough",
            F.when(F.col("pickup_borough").isNull() | (F.col("pickup_borough") == "Unknown"), "Unknown")
            .otherwise(F.col("pickup_borough"))
        )
        .withColumn(
            "dropoff_borough",
            F.when(F.col("dropoff_borough").isNull() | (F.col("dropoff_borough") == "Unknown"), "Unknown")
            .otherwise(F.col("dropoff_borough"))
        )
    )

    # Filter to strictly 2024-01-01 to 2024-06-30
    df_valid = df_enriched.filter(F.col("pickup_date").between("2024-01-01", "2024-06-30"))

    # ---------------------------------------------------------
    # 6. Window Function & Aggregations
    # ---------------------------------------------------------
    print(">>> [4/7] Computing hourly zone volumes & rolling 7-day window aggregations...")

    # (A) Hourly pickup volume by zone
    df_hourly_zone_volume = (
        df_valid
        .groupBy("pickup_date", "pickup_hour", "pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("total_amount"), 2).alias("avg_total_amount"),
            F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance")
        )
    )

    # (B) 7-day rolling average of daily trip volume
    df_daily_trips = (
        df_valid
        .groupBy("pickup_date")
        .agg(F.count("*").alias("daily_trip_count"))
    )

    window_7day = Window.orderBy("pickup_date").rowsBetween(-6, 0)
    df_daily_rolling = (
        df_daily_trips
        .withColumn("rolling_7d_avg_trips", F.round(F.avg("daily_trip_count").over(window_7day), 2))
        .orderBy("pickup_date")
    )

    # ---------------------------------------------------------
    # 7. Write Partitioned Data (Optimized with Sort)
    # ---------------------------------------------------------
    print(">>> [5/7] Writing processed data partitioned by pickup_date and pickup_borough...")
    output_path = Path(processed_base_path) / "curated_trips"
    hourly_output_path = Path(processed_base_path) / "hourly_zone_metrics"
    daily_output_path = Path(processed_base_path) / "daily_rolling_metrics"

    # Sorting within partitions guarantees single open file handles per partition
    (
        df_valid
        .repartition("pickup_date", "pickup_borough")
        .write
        .mode("overwrite")
        .partitionBy("pickup_date", "pickup_borough")
        .parquet(str(output_path))
    )

    df_hourly_zone_volume.write.mode("overwrite").parquet(str(hourly_output_path))
    df_daily_rolling.write.mode("overwrite").parquet(str(daily_output_path))

    # ---------------------------------------------------------
    # 8. Audits & Reconciliations
    # ---------------------------------------------------------
    print(">>> [6/7] Validating output and calculating reconciliation metrics...")
    df_processed = spark.read.parquet(str(output_path))
    total_processed_rows = df_processed.count()

    unmapped_pu = df_enriched.filter(F.col("PULocationID").isin([264, 265])).count()
    unmapped_do = df_enriched.filter(F.col("DOLocationID").isin([264, 265])).count()
    outlier_dates = df_enriched.filter(~F.col("pickup_date").between("2024-01-01", "2024-06-30")).count()

    print("\n================ ROW COUNT & AUDIT RECONCILIATION ================")
    print(f"Total Raw Ingested Rows       : {total_raw_rows:,}")
    print(f"Total Processed Curated Rows  : {total_processed_rows:,}")
    print(f"Null PULocationID (Before)    : {null_pu_before:,}")
    print(f"Null DOLocationID (Before)    : {null_do_before:,}")
    print(f"Unknown PULocationID (264/265): {unmapped_pu:,} ({unmapped_pu / total_raw_rows * 100:.2f}%)")
    print(f"Unknown DOLocationID (264/265): {unmapped_do:,} ({unmapped_do / total_raw_rows * 100:.2f}%)")
    print(f"Date Outliers Filtered (<Jan or >Jun): {outlier_dates:,} ({outlier_dates / total_raw_rows * 100:.3f}%)")
    print("==================================================================\n")

    print(">>> [7/7] Sample 7-day Rolling Average Output:")
    df_daily_rolling.show(14, truncate=False)

    spark.stop()
    print("Transform job completed successfully.")

if __name__ == "__main__":
    main()
