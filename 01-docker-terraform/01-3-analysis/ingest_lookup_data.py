from sqlalchemy import create_engine
import argparse
import pandas as pd
import os
from time import time

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    URL = params.url
    csv_name = 'output.csv'

    os.system(f"wget {URL} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # CSV
    df_csv_iter = pd.read_csv(csv_name, iterator=True, chunksize=10)

    # Cria a tabela com os headers (sem dados)
    df_chunk = next(df_csv_iter)
    df_chunk.head(0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insere o primeiro chunk
    t_start = time()
    df_chunk.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()
    print(f'Inserted first chunk, took {t_end - t_start:.3f} seconds')

    # Continua com os demais chunks
    for df_chunk in df_csv_iter:
        t_start = time()

        df_chunk.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()
        print(f'Inserted another chunk, took {t_end - t_start:.3f} seconds')

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