from sqlalchemy import create_engine
import argparse
import pandas as pd
import os
import pyarrow.parquet as pq
from time import time

def main(params):
    
        #parameter

        user = params.user
        password = params.password
        host = params.host
        port = params.port
        db = params.db
        table_name = params.table_name
        url = params.url
        parquet_name = 'output.parquet'

        #download parquet

        os.system(f"wget {url} -O {parquet_name}")

        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
       
        #table

        df_parquet = pq.ParquetFile(parquet_name)
        batch_iter = df_parquet.iter_batches(batch_size=10000)
        first_batch = next(batch_iter).to_pandas()
        first_batch.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        first_batch.to_sql(name=table_name, con=engine, if_exists='append')

        for batch in batch_iter:
                
                t_start = time()

                df_chunk = batch.to_pandas()
                df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

                t_end = time()

                print(f'Chunk inserted with {len(df_chunk)} lines, took {(t_end - t_start)} seconds...')

if __name__ == '__main__':

        parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

        parser.add_argument('--user', help='user name for postgres')
        parser.add_argument('--password', help='password for postgres')
        parser.add_argument('--host', help='host for postgres')
        parser.add_argument('--port', help='port name for postgres')
        parser.add_argument('--db', help='database name for postgres')
        parser.add_argument('--table_name', help='name of the table where we will write the results to')
        parser.add_argument('--url', help='url of the csv file')

        args = parser.parse_args()

        main(args)









