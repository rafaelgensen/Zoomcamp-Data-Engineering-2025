### NY Taxi Ingest and Analysis

## Docker Build taxi_ingest

docker build -t taxi_ingest:v002 .

## Docker Script taxi_ingest

# Green Taxi - October 2019 data ingestion

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2019-10.parquet"

docker run --network=13analysis_default \
    taxi_ingest:v002 \
    --user="root" \
    --password="root" \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}

# Lookup data ingestion

URL="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

docker run --network=13analysis_default \
    taxi_ingest:v002 \
    --user="root" \
    --password="root" \
    --host=pgdatabase \
    --port=5432 \
    --db=ny_taxi \
    --table_name=taxis_lookup \
    --url=${URL}
    
## Analysis

# During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:

# Up to 1 mile
# In between 1 (exclusive) and 3 miles (inclusive),
# In between 3 (exclusive) and 7 miles (inclusive),
# In between 7 (exclusive) and 10 miles (inclusive),
# Over 10 miles

select
    case
        when trip_distance <= 1 then 'Up to 1 mile'
        when trip_distance > 1 and trip_distance <= 3 then '1~3 miles'
        when trip_distance > 3 and trip_distance <= 7 then '3~7 miles'
        when trip_distance > 7 and trip_distance <= 10 then '7~10 miles'
        else '10+ miles'
    end as segment,
    to_char(count(1), '999,999') as num_trips
from
    green_taxi_trips
where
    lpep_pickup_datetime >= '2019-10-01'
    and lpep_pickup_datetime < '2019-11-01'
    and lpep_dropoff_datetime >= '2019-10-01'
    and lpep_dropoff_datetime < '2019-11-01'
group by
    segment

+--------------+----------------+
| segment      | num_trips      |
|--------------+----------------+
| Up to 1 mile | 104,802        |
| 1~3 miles    | 198,924        |
| 3~7 miles    | 109,603        |
| 7~10 miles   | 27,678         |
| 10+ miles    | 35,189         |
+--------------+----------------+


# Which was the pick up day with the longest trip distance? Use the pick up time for your calculations.

SELECT 
    lpep_pickup_datetime
FROM 
    green_taxi_trips
WHERE 
    trip_distance = (SELECT max(trip_distance) FROM green_taxi_trips)

+-------------------------+
| pickup_datetime         |
|-------------------------+
| 2019-10-31 23:23:41     |   
+-------------------------+


# Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-10-18?

SELECT 
	"Zone", ROUND(SUM(total_amount)::numeric, 2) as total_amount
FROM 
	green_taxi_trips
JOIN 
	taxis_lookup
ON 
	"PULocationID" = "LocationID"
WHERE 
	lpep_pickup_datetime::date = '2019-10-18'
GROUP BY 
	"Zone"
ORDER BY 
	total_amount DESC LIMIT 5

+-----------------------+----------------------+
| zone                  | grand_total_amount   |
+-----------------------+----------------------+
| East Harlem North     | 18686.68             |
| East Harlem South     | 16797.26             |
| Morningside Heights   | 13029.79             |
+-----------------------+----------------------+

# For the passengers picked up in October 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?

SELECT 
	t."Zone", g.tip_amount
FROM 
	(SELECT * 
	 FROM
	 	green_taxi_trips
	 WHERE 
	 	"PULocationID" = (
							SELECT 
								"PULocationID"
							FROM 
								green_taxi_trips
							JOIN
								taxis_lookup ON
								"PULocationID" = "LocationID"
							WHERE 
								"Zone" = 'East Harlem North' LIMIT 1
						  ) AND
	EXTRACT(YEAR FROM lpep_pickup_datetime) = '2019' AND
	EXTRACT(MONTH FROM lpep_pickup_datetime) = '10'					  
) g
JOIN 
	taxis_lookup t 
ON 
	g."DOLocationID" = t."LocationID"
ORDER BY 
	g.tip_amount DESC LIMIT 1

+---------------------+------------+
| dropoff_zone        | tip_amount |
+---------------------+------------|
| JFK Airport         | 87.3       |
+---------------------+------------+