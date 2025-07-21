
## PG on Bash

# pip install pgcli
# pgcli -h localhost -p 5432 -u root -d ny_taxi

## Simple data analysis on Bash

# less database_name.csv
# head -n 100 database_name.csv > database_head.csv
# wc -l database_name.csv

# Network between pgadmin and postgres

# Create Network

docker network create pg-network

## Run PGAdmin
# (install) docker pull dpage/pgadmin4

docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@admin.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8081:80 --network=pg-network \
--name pgadmin-2 \
dpage/pgadmin4

## Python Script for Data Ingestion
# jupyter nbconvert --to=script upload-data.ipynb

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

python ingest_taxi_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}

## Docker Build taxi_ingest

docker build -t taxi_ingest:v001 .

## Docker Script taxi_ingest

URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet"

docker run --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi \
    --table_name=yellow_taxi_trips \
    --url=${URL}
    
