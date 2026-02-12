-- MATERIALIZE ML-READY TABLE WITH APPROPRIATE DATA TYPES
CREATE OR REPLACE TABLE nytaxi_data.yellow_trips_ml (
  passenger_count INTEGER,
  trip_distance FLOAT64,
  PULocationID STRING,
  DOLocationID STRING,
  payment_type STRING,
  fare_amount FLOAT64,
  tolls_amount FLOAT64,
  tip_amount FLOAT64
) AS
SELECT passenger_count,
  trip_distance, 
  CAST(PULocationID AS STRING), 
  CAST(DOLocationID AS STRING), 
  CAST(payment_type AS STRING), 
  fare_amount, 
  tolls_amount, 
  tip_amount
FROM nytaxi_data.yellow_trips24_partitioned_clustered
WHERE fare_amount != 0;

-- TRAIN A SIMPLE LINEAR REGRESSION MODEL TO PREDICT TAXI TIP AMOUNT
CREATE OR REPLACE MODEL nytaxi_data.taxi_tip_model
OPTIONS
(model_type='linear_reg',
input_label_cols=['tip_amount'],
DATA_SPLIT_METHOD='AUTO_SPLIT') AS
SELECT *
FROM nytaxi_data.yellow_trips_ml
WHERE tip_amount IS NOT NULL;

-- INSPECT MODEL FEATURE INFORMATION
SELECT * 
FROM ML.FEATURE_INFO(MODEL nytaxi_data.taxi_tip_model);

-- EVALUATE MODEL PERFORMANCE
SELECT *
FROM ML.EVALUATE(MODEL nytaxi_data.taxi_tip_model,
  (
    SELECT *
    FROM nytaxi_data.yellow_trips_ml
    WHERE tip_amount IS NOT NULL
  )
);

-- GENERATE TIP AMOUNT PREDICTIONS
SELECT *
FROM ML.PREDICT(MODEL nytaxi_data.taxi_tip_model,
  (
    SELECT *
    FROM nytaxi_data.yellow_trips_ml
    WHERE tip_amount IS NOT NULL
  )
);

-- EXPLAIN MODEL PREDICTIONS WITH FEATURE CONTRIBUTIONS
SELECT *
FROM ML.EXPLAIN_PREDICT(MODEL nytaxi_data.taxi_tip_model,
  (
    SELECT *
    FROM nytaxi_data.yellow_trips_ml
    WHERE tip_amount IS NOT NULL
  ),
  STRUCT(3 as top_k_features)
);

-- HYPERPARAMETER TUNING LINEAR REGRESSION MODEL
CREATE OR REPLACE MODEL nytaxi_data.taxi_tip_model
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD="AUTO_SPLIT",
  num_trials=5,
  max_parallel_trials=2,
  l1_reg=hparam_range(0,20),
  l2_reg=hparam_candidates([0, 0.1, 1, 10])
) AS
SELECT *
FROM nytaxi_data.yellow_trips_ml
WHERE tip_amount IS NOT NULL;