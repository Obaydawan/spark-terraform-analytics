import pytest
from pathlib import Path
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession to avoid repeated JVM startup overhead."""
    session = (
        SparkSession.builder
        .appName("nyc-taxi-test-suite")
        .master("local[2]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

@pytest.fixture(scope="session")
def project_paths():
    """Canonical data paths for test validations."""
    base = Path(__file__).parent.parent
    return {
        "raw_tripdata": str(base / "data/raw/yellow_tripdata"),
        "raw_zone_lookup": str(base / "data/raw/zone_lookup/taxi_zone_lookup.csv"),
        "curated_trips": str(base / "data/processed/curated_trips"),
        "hourly_zone_metrics": str(base / "data/processed/hourly_zone_metrics"),
        "daily_rolling_metrics": str(base / "data/processed/daily_rolling_metrics"),
    }
