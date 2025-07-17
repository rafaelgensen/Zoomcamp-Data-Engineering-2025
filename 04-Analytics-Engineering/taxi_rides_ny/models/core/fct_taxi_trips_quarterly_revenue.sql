{{ 
    config(materialized='table') 
    
    }}

with trip_2019_2020 as (

  SELECT 
    sum(total_amount) as sum_total_amount,
    EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
    EXTRACT(QUARTER FROM pickup_datetime) AS pickup_quarter,
    service_type
  FROM {{ ref('fact_trips') }}
  GROUP BY pickup_year, pickup_quarter, service_type

)

SELECT
  
  service_type,
  pickup_year,
  pickup_quarter,
--  sum_total_amount,
--  LAG(sum_total_amount) OVER (PARTITION BY service_type, pickup_quarter ORDER BY pickup_quarter,pickup_year) as prev_total_amount,
  (sum_total_amount / LAG(sum_total_amount) OVER (PARTITION BY service_type, pickup_quarter ORDER BY pickup_quarter,pickup_year)) as coef_quarter_compared
  
FROM trip_2019_2020
WHERE pickup_year = 2019 OR pickup_year = 2020
ORDER BY service_type, coef_quarter_compared DESC