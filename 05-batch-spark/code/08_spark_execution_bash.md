## Local

# ./sbin/start-master.sh

# ./sbin/start-slave.sh <master-url>

URL:"spark://<URL>"

spark-submit \
	--master="${URL} \
	08_spark_loca.py \
		--input_green=data/raw/green/2021/* \
		--input_yellow=data/raw/yellow/2021/* \
		--output=data/report-2021

# ./sbin/stop-slave.sh

## GCS

gcloud dataproc jobs submit pyspark \
	--cluster=de-zoomcamp-cluster \
	--region=us-east1 \
	gs://dtc_data_lake_de-zoomcamp-nytaxi-rafael-study-2025/code/09_spark_gcs_script.py \
	-- \
		--input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi-rafael-study-2025/raw/green/2021/*/ \
                --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi-rafael-study-2025/raw/yellow/2021/*/ \ 
                --output=gs://dtc_data_lake_de-zoomcamp-nytaxi-rafael-study-2025/report-2021

## Big Query

gcloud dataproc jobs submit pyspark \
        --cluster=de-zoomcamp-cluster \
        --region=us-east1 \
	--jars=gs://spark-lib/bigquery/spark-3.4-bigquery-0.42.2.jar \
        gs://dtc_data_lake_de-zoomcamp-nytaxi-rafael-study-2025/code/10_spark_bquery_script.py \
        -- \
                --input_green=gs://dtc_data_lake_de-zoomcamp-nytaxi-rafael-study-2025/raw/green/2021/*/ \
                --input_yellow=gs://dtc_data_lake_de-zoomcamp-nytaxi-rafael-study-2025/raw/yellow/2021/*/ \
                --output=trips_data_all.reports-2021
