{{ 
    config(materialized='table') 
    
    }}

WITH trip_percentile as (

  SELECT 
    fare_amount,
    trip_distance,
    payment_type_description,
    service_type,
    EXTRACT(YEAR FROM pickup_datetime) AS pickup_year,
    EXTRACT(MONTH FROM pickup_datetime) AS pickup_month
  FROM 
    {{ ref('fact_trips') }}
  WHERE 
    fare_amount > 0 AND 
    trip_distance > 0 AND
    payment_type_description IN ('Cash', 'Credit card')

)

SELECT DISTINCT
  service_type,
  PERCENTILE_CONT(fare_amount, 0.97) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS fare_amount_p97,
  PERCENTILE_CONT(fare_amount, 0.95) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS fare_amount_p95,
  PERCENTILE_CONT(fare_amount, 0.90) OVER (PARTITION BY service_type, pickup_year, pickup_month) AS fare_amount_p90
FROM 
  trip_percentile
WHERE
  pickup_year = 2020 AND pickup_month = 04