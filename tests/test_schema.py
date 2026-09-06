import pytest

def test_curated_trips_schema(spark, project_paths):
    """Validates schema contracts on curated_trips layer."""
    df = spark.read.parquet(project_paths["curated_trips"])
    actual_dtypes = dict(df.dtypes)

    expected_types = {
        "tpep_pickup_datetime": ("timestamp", "timestamp_ntz"),
        "tpep_dropoff_datetime": ("timestamp", "timestamp_ntz"),
        "PULocationID": ("bigint", "int"),
        "DOLocationID": ("bigint", "int"),
        "trip_distance": ("double", "float"),
        "passenger_count": ("bigint", "int", "double", "float"),
        "fare_amount": ("double", "float"),
        "total_amount": ("double", "float"),
        "pickup_zone": ("string",),
        "pickup_service_zone": ("string",),
        "dropoff_borough": ("string",),
        "dropoff_zone": ("string",),
        "dropoff_service_zone": ("string",),
        "pickup_hour": ("int", "bigint"),
        "pickup_date": ("date",),
        "pickup_borough": ("string",),
    }

    for col_name, allowed_types in expected_types.items():
        assert col_name in actual_dtypes, f"Missing expected column: {col_name}"
        assert actual_dtypes[col_name] in allowed_types, (
            f"Column {col_name} expected one of {allowed_types}, found {actual_dtypes[col_name]}"
        )

def test_hourly_zone_metrics_schema(spark, project_paths):
    """Validates schema contracts on hourly_zone_metrics aggregation layer."""
    df = spark.read.parquet(project_paths["hourly_zone_metrics"])
    actual_dtypes = dict(df.dtypes)

    expected_types = {
        "pickup_date": ("date",),
        "pickup_hour": ("int", "bigint"),
        "pickup_borough": ("string",),
        "pickup_zone": ("string",),
        "trip_count": ("bigint", "int"),
        "avg_total_amount": ("double", "float"),
        "avg_trip_distance": ("double", "float"),
    }

    for col_name, allowed_types in expected_types.items():
        assert col_name in actual_dtypes, f"Missing column: {col_name}"
        assert actual_dtypes[col_name] in allowed_types, (
            f"Column {col_name} expected one of {allowed_types}, found {actual_dtypes[col_name]}"
        )

def test_daily_rolling_metrics_schema(spark, project_paths):
    """Validates schema contracts on daily_rolling_metrics layer."""
    df = spark.read.parquet(project_paths["daily_rolling_metrics"])
    actual_dtypes = dict(df.dtypes)

    expected_types = {
        "pickup_date": ("date",),
        "daily_trip_count": ("bigint", "int"),
        "rolling_7d_avg_trips": ("double", "float"),
    }

    for col_name, allowed_types in expected_types.items():
        assert col_name in actual_dtypes, f"Missing column: {col_name}"
        assert actual_dtypes[col_name] in allowed_types, (
            f"Column {col_name} expected one of {allowed_types}, found {actual_dtypes[col_name]}"
        )
