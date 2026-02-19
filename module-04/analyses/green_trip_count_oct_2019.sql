SELECT
  SUM(total_monthly_trips) AS green_trips_oct_2019
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE service_type = 'Green'
  AND revenue_month = DATE('2019-10-01')