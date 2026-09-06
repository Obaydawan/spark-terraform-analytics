import pytest
from pyspark.sql import functions as F

def test_row_count_reconciliation_ratio(spark, project_paths):
    """
    Asserts filtered rows (timestamp corruption, edge outliers)
    remain below 0.01% of raw volume.
    """
    raw_count = spark.read.parquet(project_paths["raw_tripdata"]).count()
    curated_count = spark.read.parquet(project_paths["curated_trips"]).count()

    assert raw_count > 0, "Raw trip dataset is empty"
    assert curated_count <= raw_count, "Curated count cannot exceed raw count"

    filtered_count = raw_count - curated_count
    filtered_pct = (filtered_count / raw_count) * 100.0

    # Strict bound: filtered records must be less than 0.01% of total ingest
    assert filtered_pct < 0.01, f"Filtered rate ({filtered_pct:.4f}%) exceeded 0.01% threshold"

def test_unknown_zone_rate_bounds(spark, project_paths):
    """
    Asserts Unknown LocationIDs (264/265) remain within safe bounds (<2%).
    Catches silent join drops or corrupted dimension lookups.
    """
    df_curated = spark.read.parquet(project_paths["curated_trips"])
    total_curated = df_curated.count()

    unknown_pu_count = df_curated.filter(F.col("PULocationID").isin([264, 265])).count()
    unknown_do_count = df_curated.filter(F.col("DOLocationID").isin([264, 265])).count()

    pu_rate_pct = (unknown_pu_count / total_curated) * 100.0
    do_rate_pct = (unknown_do_count / total_curated) * 100.0

    assert pu_rate_pct < 2.0, f"Pickup Unknown rate ({pu_rate_pct:.2f}%) exceeded 2% bound"
    assert do_rate_pct < 2.0, f"Dropoff Unknown rate ({do_rate_pct:.2f}%) exceeded 2% bound"

def test_zero_nulls_on_critical_columns(spark, project_paths):
    """Asserts zero nulls exist on essential routing and timestamp fields."""
    df_curated = spark.read.parquet(project_paths["curated_trips"])

    critical_cols = [
        "PULocationID",
        "DOLocationID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pickup_date",
        "pickup_borough"
    ]

    for col in critical_cols:
        null_count = df_curated.filter(F.col(col).isNull()).count()
        assert null_count == 0, f"Critical column {col} contains {null_count} nulls"
