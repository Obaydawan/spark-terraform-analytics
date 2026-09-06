# Phase 2 Spec: Spark Transform
## Project: Distributed Analytics Platform (Spark + Terraform) — NYC TLC Trip Data
Owner: Claude | Executor: Gemini

- Ingest all raw monthly parquet files as a single partitioned DataFrame.
- Explicit broadcast join with taxi zone lookup table for PULocationID and DOLocationID.
- Window functions: 7-day rolling average of daily trip volumes and hourly pickup zone aggregations.
- Write partitioned parquet by pickup_date and pickup_borough.
- Document cluster scaling architecture in docs/scaling_notes.md.
