import os
import sys
from pathlib import Path
import requests
import pyarrow.parquet as pq
import pandas as pd

TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DATA_DIR = Path("data/raw")
YELLOW_DIR = DATA_DIR / "yellow_tripdata"
ZONE_DIR = DATA_DIR / "zone_lookup"

# 6 consecutive months
MONTHS = [
    "2024-01",
    "2024-02",
    "2024-03",
    "2024-04",
    "2024-05",
    "2024-06",
]

def download_file(url: str, dest_path: Path) -> None:
    if dest_path.exists() and dest_path.stat().st_size > 0:
        print(f"[SKIP] {dest_path.name} already exists.")
        return

    print(f"[DOWNLOADING] {url} -> {dest_path}")
    headers = {"User-Agent": "Mozilla/5.0"}
    with requests.get(url, stream=True, headers=headers, timeout=60) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024 * 8):
                if chunk:
                    f.write(chunk)
    print(f"[COMPLETED] {dest_path.name} ({dest_path.stat().st_size / (1024**2):.2f} MB)")

def run_phase1_acquisition():
    YELLOW_DIR.mkdir(parents=True, exist_ok=True)
    ZONE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Download dimension table
    zone_lookup_file = ZONE_DIR / "taxi_zone_lookup.csv"
    download_file(LOOKUP_URL, zone_lookup_file)

    # 2. Download Parquet trip data
    for ym in MONTHS:
        filename = f"yellow_tripdata_{ym}.parquet"
        url = f"{TLC_BASE_URL}/{filename}"
        download_file(url, YELLOW_DIR / filename)

    # 3. Dimension Table Validation
    print("\n--- Validating Zone Lookup Table ---")
    zone_df = pd.read_csv(zone_lookup_file)
    expected_cols = {"LocationID", "Borough", "Zone", "service_zone"}
    actual_cols = set(zone_df.columns)
    
    missing_cols = expected_cols - actual_cols
    if missing_cols:
        raise ValueError(f"Zone lookup is missing expected columns: {missing_cols}")
    print(f"Zone lookup validated: {len(zone_df)} zones found. Columns: {list(zone_df.columns)}")

    # 4. Parquet Schema & Row Count Validation (Metadata-only)
    print("\n--- Inspecting Parquet Files Metadata ---")
    schemas = {}
    row_counts = {}
    file_sizes = {}
    total_size_bytes = 0

    for ym in MONTHS:
        file_path = YELLOW_DIR / f"yellow_tripdata_{ym}.parquet"
        size_bytes = file_path.stat().st_size
        total_size_bytes += size_bytes
        file_sizes[ym] = size_bytes / (1024**2)

        parquet_file = pq.ParquetFile(file_path)
        num_rows = parquet_file.metadata.num_rows
        row_counts[ym] = num_rows

        if num_rows < 100_000:
            print(f"[WARNING] {file_path.name} has only {num_rows} rows. Potentially corrupt!")

        schema = parquet_file.schema_arrow
        schemas[ym] = [(field.name, str(field.type)) for field in schema]

    total_gb = total_size_bytes / (1024**3)

    # 5. Schema Consistency Check
    base_month = MONTHS[0]
    base_schema = schemas[base_month]
    schema_mismatches = {}

    for ym in MONTHS[1:]:
        if schemas[ym] != base_schema:
            diff = {
                "added": set(schemas[ym]) - set(base_schema),
                "removed": set(base_schema) - set(schemas[ym])
            }
            schema_mismatches[ym] = diff

    if schema_mismatches:
        notes_path = DATA_DIR / "yellow_tripdata" / "schema_notes.md"
        with open(notes_path, "w") as f:
            f.write("# Schema Discrepancy Notes\n\n")
            f.write(f"Baseline Month: {base_month}\n\n")
            for ym, diff in schema_mismatches.items():
                f.write(f"### Mismatch in {ym}\n")
                f.write(f"- Differences: {diff}\n")
        print(f"[ALERT] Schema differences detected! Documented in {notes_path}")
    else:
        print("[SUCCESS] All 6 months share an identical schema.")

    # 6. Summary Report
    print("\n================ FINAL REPORT ================")
    print(f"{'Month':<10} | {'Rows':<12} | {'Size (MB)':<10}")
    print("-" * 38)
    total_rows = 0
    for ym in MONTHS:
        rows = row_counts[ym]
        total_rows += rows
        print(f"{ym:<10} | {rows:<12,d} | {file_sizes[ym]:<10.2f}")
    print("-" * 38)
    print(f"{'TOTAL':<10} | {total_rows:<12,d} | {total_gb * 1024:<10.2f} MB ({total_gb:.2f} GB)")

if __name__ == "__main__":
    run_phase1_acquisition()
