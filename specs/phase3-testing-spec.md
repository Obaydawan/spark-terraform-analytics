# Phase 3 Spec: Formal Testing & Validation
## Project: Distributed Analytics Platform (Spark + Terraform) — NYC TLC Trip Data

- conftest.py: Shared session-scoped SparkSession fixture and file paths.
- test_schema.py: Type and column validation for curated_trips, hourly_zone_metrics, and daily_rolling_metrics.
- test_reconciliation.py: Relative threshold checks (<0.01% filtered, <2% unknown zones, zero nulls on key fields).
- test_aggregations.py: Spot-checks on groupBy hourly aggregations and 7-day rolling window math (including boundary conditions).
- test_partitioning.py: Directory structure layout check and query plan partition-pruning verification via Spark plan inspection.
