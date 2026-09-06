import pytest
from pathlib import Path
from pyspark.sql import functions as F

def test_physical_partition_layout(project_paths):
    """
    Asserts directory on disk has physical Hive-style partition folders:
    data/processed/curated_trips/pickup_date=YYYY-MM-DD/pickup_borough=...
    """
    base_dir = Path(project_paths["curated_trips"])
    assert base_dir.exists(), "curated_trips directory does not exist"

    date_partitions = [p for p in base_dir.iterdir() if p.is_dir() and p.name.startswith("pickup_date=")]
    assert len(date_partitions) > 100, f"Expected 182 date partitions, found {len(date_partitions)}"

    sample_date = date_partitions[0]
    borough_partitions = [p for p in sample_date.iterdir() if p.is_dir() and p.name.startswith("pickup_borough=")]
    assert len(borough_partitions) > 0, f"No pickup_borough partitions found in {sample_date.name}"

def test_partition_pruning_explain_plan(spark, project_paths):
    """
    Asserts that filtering on a partition key (pickup_date) pushes the filter
    into Spark's FileScan PartitionFilters, avoiding full dataset scans.
    """
    df = spark.read.parquet(project_paths["curated_trips"])
    filtered_df = df.filter(F.col("pickup_date") == "2024-03-01")

    # Capture physical execution plan
    executed_plan = filtered_df._jdf.queryExecution().executedPlan().toString()

    # In Spark physical plans, PartitionFilters appear in the FileScan node
    assert "PartitionFilters" in executed_plan, "No PartitionFilters detected in physical plan"
    assert "pickup_date" in executed_plan, "PartitionFilter does not contain pickup_date"
    
    # Confirm PartitionFilters explicitly contains the predicate '2024-03-01'
    assert "2024-03-01" in executed_plan, "Target partition date '2024-03-01' missing from PartitionFilters"
