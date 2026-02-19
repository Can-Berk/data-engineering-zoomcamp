with source as (
    select * from {{ source('raw_data', 'green_tripdata') }}
),

renamed as (
    SELECT
        -- identifiers
            CAST(vendorid AS integer) AS vendor_id,
            {{ safe_cast('ratecodeid', 'integer') }} AS rate_code_id,
            CAST(pulocationid AS integer) AS pickup_location_id,
            CAST(dolocationid AS integer) AS dropoff_location_id,

            -- timestamps
            CAST(lpep_pickup_datetime AS timestamp) AS pickup_datetime,  -- lpep = Licensed Passenger Enhancement Program (green taxis)
            CAST(lpep_dropoff_datetime AS timestamp) AS dropoff_datetime,

            -- trip info
            CAST(store_and_fwd_flag AS string) AS store_and_fwd_flag,
            CAST(passenger_count AS integer) AS passenger_count,
            CAST(trip_distance AS numeric) AS trip_distance,
            {{ safe_cast('trip_type', 'integer') }} AS trip_type,

            -- payment info
            CAST(fare_amount AS numeric) AS fare_amount,
            CAST(extra AS numeric) AS extra,
            CAST(mta_tax AS numeric) AS mta_tax,
            CAST(tip_amount AS numeric) AS tip_amount,
            CAST(tolls_amount AS numeric) AS tolls_amount,
            CAST(ehail_fee AS numeric) AS ehail_fee,
            CAST(improvement_surcharge AS numeric) AS improvement_surcharge,
            CAST(total_amount AS numeric) AS total_amount,
            {{ safe_cast('payment_type', 'integer') }} AS payment_type
        FROM source
        WHERE vendorid IS NOT NULL
)

SELECT * FROM renamed

-- Sample records for dev environment using deterministic date filter
{% if target.name == 'dev' %}
WHERE pickup_datetime >= '2019-01-01' AND pickup_datetime < '2019-02-01'
{% endif %}