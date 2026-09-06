# Phase 1 Spec: Data Acquisition & Landing Zone
## Project: Distributed Analytics Platform (Spark + Terraform) — NYC TLC Trip Data
Owner: Claude | Executor: Gemini

- Ingested 6 months (2024-01 through 2024-06) of Yellow Taxi Trip Records (20.33M rows, 326MB Snappy-compressed).
- Landed taxi_zone_lookup.csv dimension table (265 zones).
- Validated identical schema across all 6 raw parquet files via pyarrow metadata reads.
