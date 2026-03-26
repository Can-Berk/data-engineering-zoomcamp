import dataclasses
import json
import sys
from time import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from kafka import KafkaProducer
from models import Ride, ride_from_row

# Download NYC green taxi trip data October 2025
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-10.parquet"
columns = [
    'lpep_pickup_datetime',
    'lpep_dropoff_datetime',
    'PULocationID',
    'DOLocationID',
    'passenger_count',
    'trip_distance',
    'tip_amount',
    'total_amount'
]

df = pd.read_parquet(url, columns=columns)

# Convert datetime to string
df['lpep_pickup_datetime'] = df['lpep_pickup_datetime'].astype(str)
df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].astype(str)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'green-trips'

t0 = time()

for _, row in df.iterrows():
    producer.send(topic, value=row.to_dict())

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')