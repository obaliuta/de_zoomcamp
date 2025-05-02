#!/usr/bin/env python
# coding: utf-8
import os
from time import time
import pandas as pd
from sqlalchemy import create_engine

def ingest_data(user, password, host, port, db, table_name, url):
    # Determine the CSV filename based on extension
    csv_name = 'yellow_tripdata_2021-01.csv.gz' if url.endswith('.csv.gz') else 'output.csv'
    
    # Download the CSV file
    os.system(f"wget {url} -O {csv_name}")

    # Construct the PostgreSQL connection URL
    postgres_url = f'postgresql://{user}:{password}@{host}:{port}/{db}'
    # Create the SQLAlchemy engine (no future=True flag so that we use the legacy connection API)
    engine = create_engine(postgres_url)
    
    # Read the CSV file in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    # Process the first chunk: convert date columns
    df = next(df_iter)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    
    # Create (or replace) the target table using an empty dataframe (schema only)
    # Use engine.connect() so that the returned connection has proper dialect support.
    with engine.connect() as conn:
        df.head(0).to_sql(name=table_name, con=conn, if_exists='replace', index=False)
    
    # Insert the first chunk into the target table
    with engine.connect() as conn:
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
    
    # Process subsequent chunks and append each to the target table
    while True:
        try:
            t_start = time()
            
            df = next(df_iter)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            
            with engine.connect() as conn:
                df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
            
            t_end = time()
            print(f"Inserted another chunk, took {t_end - t_start:.3f} seconds")
        except StopIteration:
            print("Finished ingesting data into the PostgreSQL database")
            break

if __name__ == '__main__':
    # Connection parameters and CSV URL – make sure PostgreSQL is running on localhost:5432
    user = "root"
    password = "root"
    host = "localhost"       # If PostgreSQL is installed locally
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
    
    ingest_data(user, password, host, port, db, table_name, csv_url)
