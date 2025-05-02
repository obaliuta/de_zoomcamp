#!/usr/bin/env python
# coding: utf-8
import os
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, retry_delay_seconds=5, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(url):
    # Determine the CSV filename based on extension
    csv_name = 'yellow_tripdata_2021-01.csv.gz' if url.endswith('.csv.gz') else 'output.csv'
    
    # Download the CSV file
    os.system(f"wget {url} -O {csv_name}")
    
    # Read the CSV file in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    return df

@task(log_prints=True, retries=3, retry_delay_seconds=5)
def transform_data(df):
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df

@task(log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data(table_name: str, df: pd.DataFrame):
    connection_block = SqlAlchemyConnector.load("sqlalchemypostgres")

    with connection_block.get_connection(begin=False) as engine:
        # Use engine.connect() so that the returned connection has proper dialect support.
        with engine.connect() as conn:
            df.head(0).to_sql(name=table_name, con=conn, if_exists='replace', index=False)
        
        # Insert the first chunk into the target table
        with engine.connect() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)

@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"logging a subflow for {table_name}")


@flow(name="Ingest Flow")
def main_flow(table_name : str):
    #user = "root"
    #password = "root"
    #host = "localhost"       # If PostgreSQL is installed locally
    #port = "5432"
    #db = "ny_taxi" - all above not needed as connection block is used
    #table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    
    ingest_data(table_name, data)    

if __name__ == '__main__':
    main_flow("yellow_taxi_trips")
