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
    url = params.url
    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #table

    df_csv = pd.read_csv(csv_name)
    df_csv.head(n=0).to_sql(name= table_name, con= engine, if_exists = 'replace')
    df_csv.to_sql(name=table_name, con= engine, if_exists = 'append')
    

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