import pytest
from pyspark.sql import functions as F

def test_hourly_aggregation_spot_check(spark, project_paths):
    """
    Validates groupBy consistency between curated records and hourly_zone_metrics.
    Samples '2024-03-15', hour 14, in 'JFK Airport'.
    """
    sample_date = "2024-03-15"
    sample_hour = 14
    sample_zone = "JFK Airport"

    # Compute expected count directly from curated data
    df_curated = spark.read.parquet(project_paths["curated_trips"])
    expected_row = (
        df_curated
        .filter(
            (F.col("pickup_date") == sample_date) &
            (F.col("pickup_hour") == sample_hour) &
            (F.col("pickup_zone") == sample_zone)
        )
        .groupBy("pickup_date", "pickup_hour", "pickup_zone")
        .agg(
            F.count("*").alias("expected_count"),
            F.round(F.avg("total_amount"), 2).alias("expected_avg_total")
        )
        .first()
    )

    # Read pre-aggregated metric
    df_hourly = spark.read.parquet(project_paths["hourly_zone_metrics"])
    actual_row = (
        df_hourly
        .filter(
            (F.col("pickup_date") == sample_date) &
            (F.col("pickup_hour") == sample_hour) &
            (F.col("pickup_zone") == sample_zone)
        )
        .first()
    )

    assert expected_row is not None, "Spot check target row not found in curated data"
    assert actual_row is not None, "Spot check target row not found in hourly metrics layer"
    assert actual_row["trip_count"] == expected_row["expected_count"], (
        f"Mismatch in trip count: expected {expected_row['expected_count']}, got {actual_row['trip_count']}"
    )
    assert actual_row["avg_total_amount"] == expected_row["expected_avg_total"], (
        f"Mismatch in avg total amount: expected {expected_row['expected_avg_total']}, got {actual_row['avg_total_amount']}"
    )

def test_rolling_7d_window_math(spark, project_paths):
    """
    Verifies rolling 7-day average calculation:
    1. The first date (2024-01-01) rolling avg equals its own daily count.
    2. A mid-point date (2024-01-14) rolling avg strictly equals the arithmetic
       mean of its count and the preceding 6 consecutive days.
    """
    df_rolling = (
        spark.read.parquet(project_paths["daily_rolling_metrics"])
        .orderBy("pickup_date")
        .collect()
    )

    assert len(df_rolling) >= 7, "Daily metrics dataframe does not have enough days for window test"

    # Edge case: Day 1 (no preceding rows in rowsBetween(-6, 0))
    day_one = df_rolling[0]
    assert day_one["rolling_7d_avg_trips"] == float(day_one["daily_trip_count"]), (
        f"Day 1 rolling average ({day_one['rolling_7d_avg_trips']}) "
        f"does not match day 1 count ({day_one['daily_trip_count']})"
    )

    # Target: 14th day (index 13)
    target_idx = 13
    target_day = df_rolling[target_idx]
    
    # Preceding 6 days + target day = 7 days total
    window_slice = df_rolling[target_idx - 6 : target_idx + 1]
    expected_avg = round(sum(row["daily_trip_count"] for row in window_slice) / 7.0, 2)

    assert round(target_day["rolling_7d_avg_trips"], 2) == expected_avg, (
        f"Rolling average mismatch on {target_day['pickup_date']}: "
        f"expected {expected_avg}, got {target_day['rolling_7d_avg_trips']}"
    )
