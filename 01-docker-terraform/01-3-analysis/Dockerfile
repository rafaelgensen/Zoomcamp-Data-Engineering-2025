FROM python:3.9

WORKDIR /app

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow
COPY ingest_taxi_data.py ingest_taxi_data.py

ENTRYPOINT [ "python", "ingest_taxi_data.py" ]