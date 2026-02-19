SELECT COUNT(*) AS num_records
FROM {{ ref('fct_monthly_zone_revenue') }} --data-workflows-demo.dbt_cberk.fct_monthly_zone_revenue