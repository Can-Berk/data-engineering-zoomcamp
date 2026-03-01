"""@bruin

# Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# Set the connection.
connection: duckdb-default

# Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # choose `table` or `view` (ingestion generally should be a table)
  type: table
  # pick a strategy.
  # suggested strategy: append
  strategy: append

# Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
  - name: dropoff_datetime
    type: timestamp
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: double
  - name: payment_type
    type: integer
  - name: taxi_type
    type: varchar

@bruin"""

import os
import json
import pandas as pd
import pyarrow as pa
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

def _ensure_windows_tzdata():
    # Only relevant on Windows
    if os.name != "nt":
        return
    try:
        # downloads tzdata into the expected location if missing
        pa.util.download_tzdata_on_windows()
    except Exception:
        # If it fails, we'll handle via Option B below
        pass

# Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
import os
import json
import pandas as pd


def _ensure_windows_tzdata():
    # Fix for: pyarrow ArrowInvalid: Cannot locate timezone 'UTC' on Windows
    if os.name != "nt":
        return
    try:
        import pyarrow as pa
        pa.util.download_tzdata_on_windows()
    except Exception:
        # If this fails, install tzdata as a package and/or fix PYARROW_TZDATA.
        pass


def materialize():
    _ensure_windows_tzdata()

    start_date = pd.to_datetime(os.environ["BRUIN_START_DATE"]).tz_localize(None)
    end_date = pd.to_datetime(os.environ["BRUIN_END_DATE"]).tz_localize(None)

    vars_ = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_.get("taxi_types", ["yellow", "green"])

    allowed = {"yellow", "green"}
    bad = [t for t in taxi_types if t not in allowed]
    if bad:
        raise ValueError(f"Only {sorted(allowed)} are supported, got: {bad}")

    # TLC parquet column mapping
    pickup_cols = {"yellow": "tpep_pickup_datetime", "green": "lpep_pickup_datetime"}
    dropoff_cols = {"yellow": "tpep_dropoff_datetime", "green": "lpep_dropoff_datetime"}

    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    months = pd.date_range(start=start_date, end=end_date, freq="MS")
    if len(months) == 0:
        months = pd.DatetimeIndex([start_date.replace(day=1)])

    frames = []

    for taxi_type in taxi_types:
        pcol = pickup_cols[taxi_type]
        dcol = dropoff_cols[taxi_type]

        for dt in months:
            y = dt.year
            m = f"{dt.month:02d}"
            url = f"{base_url}/{taxi_type}_tripdata_{y}-{m}.parquet"

            df = pd.read_parquet(url)

            # These exist in yellow/green TLC parquet
            needed = [pcol, dcol, "PULocationID", "DOLocationID", "fare_amount", "payment_type"]
            missing = [c for c in needed if c not in df.columns]
            if missing:
                raise KeyError(f"Missing columns {missing} in {url}")

            out = df[needed].rename(
                columns={
                    pcol: "pickup_datetime",
                    dcol: "dropoff_datetime",
                    "PULocationID": "pickup_location_id",
                    "DOLocationID": "dropoff_location_id",
                }
            )

            # Ensure timezone-naive datetimes in pandas
            out["pickup_datetime"] = pd.to_datetime(out["pickup_datetime"], errors="coerce").dt.tz_localize(None)
            out["dropoff_datetime"] = pd.to_datetime(out["dropoff_datetime"], errors="coerce").dt.tz_localize(None)

            # Basic cleaning
            out = out.dropna(subset=["pickup_datetime", "dropoff_datetime"])
            out = out[(out["pickup_datetime"] >= start_date) & (out["pickup_datetime"] < end_date)]

            # Add taxi_type as a column for downstream
            out["taxi_type"] = taxi_type

            # Type stabilization
            out["pickup_location_id"] = pd.to_numeric(out["pickup_location_id"], errors="coerce").astype("Int64")
            out["dropoff_location_id"] = pd.to_numeric(out["dropoff_location_id"], errors="coerce").astype("Int64")
            out["payment_type"] = pd.to_numeric(out["payment_type"], errors="coerce").astype("Int64")
            out["fare_amount"] = pd.to_numeric(out["fare_amount"], errors="coerce")

            frames.append(out)

    if frames:
        final_df = pd.concat(frames, ignore_index=True)
        # convert nullable Int64 -> plain int where possible (duckdb handles nulls too, but this is safer)
        final_df["pickup_location_id"] = final_df["pickup_location_id"].astype("float").astype("Int64")
        final_df["dropoff_location_id"] = final_df["dropoff_location_id"].astype("float").astype("Int64")
        final_df["payment_type"] = final_df["payment_type"].astype("float").astype("Int64")
        return final_df

    return pd.DataFrame(
        columns=[
            "pickup_datetime",
            "dropoff_datetime",
            "pickup_location_id",
            "dropoff_location_id",
            "fare_amount",
            "payment_type",
            "taxi_type",
        ]
    )