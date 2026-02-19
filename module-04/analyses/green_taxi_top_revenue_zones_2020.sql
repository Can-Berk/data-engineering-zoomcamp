SELECT
  pickup_zone,
  SUM(revenue_monthly_total_amount) AS total_2020_revenue
FROM {{ ref('fct_monthly_zone_revenue') }}
WHERE service_type = 'Green'
  AND revenue_month >= DATE('2020-01-01')
  AND revenue_month < DATE('2021-01-01')
GROUP BY pickup_zone
ORDER BY total_2020_revenue DESC