#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine, inspect
from tqdm.auto import tqdm
import click
import pyarrow.parquet as pq
import urllib.request

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]



@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL username')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5432', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--year', default=2021, type=int, help='Year of the data')
@click.option('--month', default=1, type=int, help='Month of the data')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')

def main(pg_user, pg_pass, pg_host, pg_port, pg_db, year, month, chunksize, target_table):
    """Ingest NYC taxi data into PostgreSQL database."""
    prefix = 'https://d37ci6vzurychx.cloudfront.net/trip-data'
    url = f'{prefix}/green_tripdata_{year}-{month:02d}.parquet'
    url_zones = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    zones_df = pd.read_csv(url_zones)
    
    zones_df.to_sql(
    name="taxi_zones",
    con=engine,
    if_exists="replace",
    index=False
    )

    local_path = "data.parquet"
    urllib.request.urlretrieve(url, local_path)

    pq_file = pq.ParquetFile(local_path)

    # Create table only if it doesn't exist
    inspector = inspect(engine)
    table_exists = inspector.has_table(target_table)

    for batch in tqdm(pq_file.iter_batches(batch_size=chunksize)):
        df_chunk = batch.to_pandas()

        # optional: enforce datetime parsing if needed
        # for col in parse_dates:
        #     if col in df_chunk.columns:
        #         df_chunk[col] = pd.to_datetime(df_chunk[col])

        if not table_exists:
            df_chunk.head(0).to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",
                index=False
            )
            table_exists = True

        df_chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists="append",
            index=False,
            method="multi",      # faster inserts
            chunksize=10_000     # controls insert batch size
        )

if __name__ == '__main__':
    main()