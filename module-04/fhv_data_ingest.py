"""
Usage examples:

Upload to GCS only:
  python fhv_data_ingest.py --bucket data-workflows-demo-lake --year 2019

Upload to GCS + BigQuery:
  python fhv_data_ingest.py --bucket data-workflows-demo-lake --year 2019 \
    --bq_table data-workflows-demo.fhv_dataset.fhv_2019

Single month:
  python fhv_data_ingest.py --bucket data-workflows-demo-lake --year 2019 --month 3 \
    --bq_table data-workflows-demo.fhv_dataset.fhv_2019
"""

from google.cloud import storage
from google.cloud import bigquery
import requests
import argparse

BASE_URL  = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/"

# --- BigQuery schemas ---
# Staging keeps datetimes as STRING to avoid load failures due to formatting quirks.
FHV_STAGE_SCHEMA = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "STRING"),
    bigquery.SchemaField("dropOff_datetime", "STRING"),
    bigquery.SchemaField("PUlocationID", "STRING"),
    bigquery.SchemaField("DOlocationID", "STRING"),
    bigquery.SchemaField("SR_Flag", "STRING"),
    bigquery.SchemaField("Affiliated_base_number", "STRING"),
]

# Final table uses typed columns.
FHV_FINAL_SCHEMA = [
    bigquery.SchemaField("dispatching_base_num", "STRING"),
    bigquery.SchemaField("pickup_datetime", "TIMESTAMP"),
    bigquery.SchemaField("dropoff_datetime", "TIMESTAMP"),
    bigquery.SchemaField("pickup_location_id", "INT64"),
    bigquery.SchemaField("dropoff_location_id", "INT64"),
    bigquery.SchemaField("sr_flag", "INT64"),
    bigquery.SchemaField("affiliated_base_number", "STRING"),
    bigquery.SchemaField("source_file", "STRING"),
]


def months_list(month=None):
    if month is not None:
        return [f"{int(month):02d}"]
    return [f"{m:02d}" for m in range(1, 13)]       # month strings in two-digit format - two digits, pad with zero


def get_data_to_cloud(url, bucket_name, blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    if blob.exists():
        print(f"Already exsists, skipping: gs://{bucket_name}/{blob_name}")
        return False        # skipped if exists

    r = requests.get(url, stream=True, timeout=60)      # Don’t download the response into RAM, alternatively iter_content()
    r.raise_for_status()
    
    blob.upload_from_file(r.raw)
    print(f"Uploaded gs://{bucket_name}/{blob_name}")
    return True

def create_bq_tables(final_table_id, staging_table_id):
    """
    Create final + staging tables if they don't exist.
    Final is partitioned by pickup_datetime and clustered for common filters.
    """
    bq = bigquery.Client()

    # Final table (partitioned)
    final = bigquery.Table(final_table_id, schema=FHV_FINAL_SCHEMA)
    final.time_partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="pickup_datetime"
    )

    final.clustering_fields = ["dispatching_base_num", "pickup_location_id", "dropoff_location_id"]
    bq.create_table(final, exists_ok=True)

    # Staging table (simple, overwritten each month)
    stage = bigquery.Table(staging_table_id, schema=FHV_STAGE_SCHEMA)
    bq.create_table(stage, exists_ok=True)

    print(f"Created BigQuery tables:\n  final: {final_table_id}\n  stage: {staging_table_id}")

def load_gcs_to_bigquery_stage(gcs_uri, staging_table_id):
    """
    Load CSV.GZ from GCS into staging table (WRITE_TRUNCATE each time).
    """

    bq = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=FHV_STAGE_SCHEMA,
    write_disposition="WRITE_TRUNCATE",
    allow_quoted_newlines=True,
    )

    job = bq.load_table_from_uri(gcs_uri, staging_table_id, job_config=job_config)
    job.result()
    print(f"Loaded to staging: {staging_table_id} <- {gcs_uri}")


def transform_stage_to_final(staging_table_id, final_table_id, source_file):
    """
    Transform rows from staging (all STRINGs) into final typed schema and append to final table.
    Handles empty strings in PU/DO IDs and SR_Flag, and safely parses timestamps.

    Idempotent per source_file: deletes existing rows for that file then inserts.
    """
    bq = bigquery.Client()

    sql = f"""
    -- Make reruns safe per file
    DELETE FROM `{final_table_id}`
    WHERE source_file = @source_file;    -- Deletes any existing rows that came from the same file

    INSERT INTO `{final_table_id}` (
      dispatching_base_num,
      pickup_datetime,
      dropoff_datetime,
      pickup_location_id,
      dropoff_location_id,
      sr_flag,
      affiliated_base_number,
      source_file
    )
    SELECT
      dispatching_base_num,

      -- Safely parse ISO "YYYY-MM-DD HH:MM:SS (with/without seconds; day may be 1 or 01)
      COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', pickup_datetime),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M',    pickup_datetime)
      ) AS pickup_datetime,

      COALESCE(
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dropOff_datetime),
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M',    dropOff_datetime)
      ) AS dropoff_datetime,

      -- Empty string -> NULL -> INT64
      SAFE_CAST(NULLIF(PULocationID, '') AS INT64) AS pickup_location_id,
      SAFE_CAST(NULLIF(DOLocationID, '') AS INT64) AS dropoff_location_id,

      -- SR_Flag mostly empty; convert '' to NULL and cast
      SAFE_CAST(NULLIF(SR_Flag, '') AS INT64) AS sr_flag,

      Affiliated_base_number AS affiliated_base_number,

      @source_file AS source_file
    FROM `{staging_table_id}`;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source_file", "STRING", source_file),
        ]
    )

    job = bq.query(sql, job_config=job_config)
    job.result()
    print(f"Transformed & loaded final: {final_table_id} (source_file={source_file})")


def ingest_data(bucket, year, month=None, bq_table=None):
    """
    If bq_table is provided, loads into BigQuery too.
    - Upload to GCS is skipped if object exists (no overwrite).
    - BigQuery load runs only when upload happened (prevents duplicate loads on rerun).
    """

    stage_table = None
    if bq_table is not None:
        stage_table = f"{bq_table}__stage"
        create_bq_tables(final_table_id=bq_table, staging_table_id=stage_table)

    for m in months_list(month):
        filename = f"fhv_tripdata_{year}-{m}.csv.gz"
        url = BASE_URL  + filename
        blob_name = f"fhv/{year}/{filename}"
        gcs_uri = f"gs://{bucket}/{blob_name}"

        try:
            uploaded = get_data_to_cloud(url, bucket, blob_name)
        except requests.HTTPError:
            print(f"Skipping missing file: {filename}")
            continue

        # BigQuery load/transform runs whenever --bq_table is provided; final load is idempotent per source_file (delete+insert).
        if bq_table:       # if bq_table and uploaded
            load_gcs_to_bigquery_stage(gcs_uri, stage_table)
            transform_stage_to_final(stage_table, bq_table, source_file=gcs_uri)

def main():
    parser = argparse.ArgumentParser(description="Load FHV CSV files to GCS")

    parser.add_argument(
        "--bucket",
        type=str,
        required=True,
        help="GCS Bucket name (gs://)"
    )

    parser.add_argument(
        "--year",
        type=int,
        required=True,
        help="Year of data (e.g. 2019)"
    )

    parser.add_argument(
        "--month",
        type=int,
        help="Month (1-12). If omitted, loads full year."
    )

    parser.add_argument(
        "--bq_table",
        type=str,
        help="Optional BigQuery table: 'project.dataset.table' (loads after upload)"
    )

    args = parser.parse_args()

    ingest_data(args.bucket, args.year, args.month, args.bq_table)

if __name__ == "__main__":
    main()
