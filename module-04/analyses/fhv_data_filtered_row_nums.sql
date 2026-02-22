SELECT COUNT(*) AS row_num
FROM {{ ref('stg_fhv_tripdata')}}