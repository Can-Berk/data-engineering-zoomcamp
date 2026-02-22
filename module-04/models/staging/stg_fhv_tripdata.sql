with source as (
    select * from {{ source('raw_data', 'fhv_2019') }}
),

renamed as (
    SELECT *
FROM source
WHERE dispatching_base_num IS NOT NULL
)

SELECT * FROM renamed