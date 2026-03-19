# # Kafka (Redpanda) and PyFlink Stream Processing Exercises

# Question 1: Redpanda version

Run `rpk version` inside the Redpanda container:

```
docker exec -it workshop-redpanda-1 rpk version
```

What version of Redpanda are you running?

**Solution:** v25.3.9.


# Question 2. Sending data to Redpanda

Create a topic called `green-trips`:

```
docker exec -it workshop-redpanda-1 rpk topic create green-trips
```

Now write a producer to send the green taxi data to this topic.

Read the parquet file and keep only these columns:

- lpep_pickup_datetime
- lpep_dropoff_datetime
- PULocationID
- DOLocationID
- passenger_count
- trip_distance
- tip_amount
- total_amount

Convert each row to a dictionary and send it to the `green-trips` topic. You'll need to handle the datetime columns - convert them to strings before serializing to JSON.

Measure the time it takes to send the entire dataset and flush:

```
from time import time

t0 = time()

# send all rows ...

producer.flush()

t1 = time()
print(f'took {(t1 - t0):.2f} seconds')
```

How long did it take to send the data?

**Solution:** 10 seconds.