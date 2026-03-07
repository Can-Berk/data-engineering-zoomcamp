# PySpark Batch Processing Exercises

This document contains the solutions from [`spark_exercises.ipynb`](./spark_exercises.ipynb) for the PySpark batch processing exercises from the Data Engineering Zoomcamp 2026. The exercises focus on working with Spark DataFrames, repartitioning data, and analyzing the NYC Yellow Taxi dataset using Apache Spark.

# Question 1: Install Spark and PySpark

- Install Spark

- Run PySpark

- Create a local spark session

- Execute spark.version.

**Solution:** Spark version: **4.1.1**


```python
import pyspark
import os
from pyspark.sql import SparkSession
import urllib.request
import glob
from pyspark.sql.functions import expr, max
from pyspark.sql import functions as F
from pyspark.sql import types
from datetime import datetime

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("test")
    .getOrCreate()
)
```


```python
print(f"Spark version: {spark.version}")
```

    Spark version: 4.1.1
    

# Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

**Solution:** The average file size is approximately **25 MB**.


```python
url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet"
output = "yellow_tripdata_2025-11.parquet"
urllib.request.urlretrieve(url, output)
```




    ('yellow_tripdata_2025-11.parquet', <http.client.HTTPMessage at 0x1d0e0fa6b40>)




```python
df_trips = spark.read.parquet("yellow_tripdata_2025-11.parquet")
```



```python
df_trips.repartition(4).write.mode("overwrite").parquet('yellow_tripdata/2025/11')
```


```python
files = glob.glob("yellow_tripdata/2025/11/*.parquet")

sizes = [os.path.getsize(f) / (1024*1024) for f in files]  # MB
avg_size = sum(sizes) / len(sizes)

print("Average size:", round(avg_size, 4), "MB")
```

    Average size: 24.4152 MB
    

# Question 3: Count records

How many taxi trips were there on the 15th of November?

Consider only trips that started on the 15th of November.

**Solution:** 162604.


```python
df_trips = df_trips.withColumn('pickup_date', F.to_date(df_trips.tpep_pickup_datetime))
df_trips.filter(df_trips.pickup_date == '2025-11-15').count()
```




    162604



# Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

**Solution:** 90.64 hours.


```python
df_trips.select(
    (expr("timestampdiff(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime)") / 3600).alias("duration_hours")
).agg(max("duration_hours")).show()
```

    +-------------------+
    |max(duration_hours)|
    +-------------------+
    |  90.64666666666666|
    +-------------------+
    
    

# Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

**Solution:** The Apache Spark Web UI runs locally on port **4040** by default.

# Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

**Solution:** The least frequent pickup zones each have 1 trip. These zones are Governor's Island/Ellis Island/Liberty Island, Eltingville/Annadale/Prince's Bay, and Arden Heights.


```python
url2 = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
output2 = "taxi_zone_lookup.csv"
urllib.request.urlretrieve(url2, output2)
```




    ('taxi_zone_lookup.csv', <http.client.HTTPMessage at 0x1d0e11b5700>)




```python
df_zones = spark.read.csv("taxi_zone_lookup.csv", header=True, inferSchema=True)
```


```python
df_trips.createOrReplaceTempView('trips')
df_zones.createOrReplaceTempView('zones')
```


```python
zone_counts = df_trips.join(
    df_zones,
    df_trips.PULocationID == df_zones.LocationID
).groupBy("Zone").count()
zone_counts.orderBy("count").show(10, False)
```

    +---------------------------------------------+-----+
    |Zone                                         |count|
    +---------------------------------------------+-----+
    |Governor's Island/Ellis Island/Liberty Island|1    |
    |Eltingville/Annadale/Prince's Bay            |1    |
    |Arden Heights                                |1    |
    |Port Richmond                                |3    |
    |Rikers Island                                |4    |
    |Rossville/Woodrow                            |4    |
    |Great Kills                                  |4    |
    |Green-Wood Cemetery                          |4    |
    |Jamaica Bay                                  |5    |
    |Westerleigh                                  |12   |
    +---------------------------------------------+-----+
    only showing top 10 rows
    
