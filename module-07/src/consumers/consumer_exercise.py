import json
import psycopg2
from datetime import datetime
from kafka import KafkaConsumer

server = 'localhost:9092'
topic_name = 'green-trips'

conn = psycopg2.connect(
    host='localhost',
    port=5432,
    database='postgres',
    user='postgres',
    password='postgres'
)
conn.autocommit = True
cur = conn.cursor()

consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=[server],
    auto_offset_reset='earliest',
    group_id='green-trips-to-postgres',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print(f"Listening to {topic_name} and writing to PostgreSQL...")

count = 0

for message in consumer:
    ride = message.value

    pickup_dt = datetime.fromisoformat(ride['lpep_pickup_datetime'])
    dropoff_dt = datetime.fromisoformat(ride['lpep_dropoff_datetime'])

    cur.execute(
        """
        INSERT INTO green_trips_processed (
            lpep_pickup_datetime,
            lpep_dropoff_datetime,
            PULocationID,
            DOLocationID,
            passenger_count,
            trip_distance,
            tip_amount,
            total_amount
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            pickup_dt,
            dropoff_dt,
            ride['PULocationID'],
            ride['DOLocationID'],
            ride['passenger_count'],
            ride['trip_distance'],
            ride['tip_amount'],
            ride['total_amount'],
        )
    )

    count += 1
    if count % 10000 == 0:
        print(f"Inserted {count} rows...")

consumer.close()
cur.close()
conn.close()