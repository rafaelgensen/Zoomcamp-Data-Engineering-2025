{{
    config(
        materialized='table'
    )
}}

with green_tripdata as (
    select *, 
        'Green' as service_type
    from {{ ref('stg_green_tripdata') }}
), 
yellow_tripdata as (
    select *, 
        'Yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
), 
trips_unioned as (
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select  

    -- General view
    CAST(trips_unioned.vendorid AS INT64) AS vendorid, 
    trips_unioned.service_type,
    CAST(trips_unioned.ratecodeid AS INT64) AS ratecodeid, 
    CAST(trips_unioned.pickup_locationid AS INT64) AS pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    CAST(trips_unioned.dropoff_locationid AS INT64) AS dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    CAST(trips_unioned.pickup_datetime AS timestamp) AS pickup_datetime, 
    CAST(trips_unioned.dropoff_datetime AS timestamp) AS dropoff_datetime, 
    trips_unioned.store_and_fwd_flag, 
    CAST(trips_unioned.passenger_count AS INT64) AS passenger_count, 
    CAST(trips_unioned.trip_distance AS NUMERIC) AS trip_distance, 
    trips_unioned.trip_type AS trip_type, 
    CAST(trips_unioned.fare_amount AS NUMERIC) AS fare_amount, 
    CAST(trips_unioned.extra AS NUMERIC) AS extra, 
    CAST(trips_unioned.mta_tax AS NUMERIC) AS mta_tax, 
    CAST(trips_unioned.tip_amount AS NUMERIC) AS tip_amount, 
    CAST(trips_unioned.tolls_amount AS NUMERIC) AS tolls_amount, 
    CAST(trips_unioned.ehail_fee AS NUMERIC) AS ehail_fee, 
    CAST(trips_unioned.improvement_surcharge AS NUMERIC) AS improvement_surcharge, 
    CAST(trips_unioned.total_amount AS NUMERIC) AS total_amount, 
    trips_unioned.payment_type AS payment_type,
    trips_unioned.payment_type_description

from trips_unioned
inner join dim_zones as pickup_zone
on trips_unioned.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on trips_unioned.dropoff_locationid = dropoff_zone.locationid