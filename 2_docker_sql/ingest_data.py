import os
import gzip
import shutil
import argparse
import time
import pandas as pd
from sqlalchemy import create_engine

def main(params):
    user = params.user
    pw = params.password
    host = params.host
    port = params.port
    db = params.database
    table = params.table
    url = params.csv_url
    archive_name = "data.csv.gz"
    csv_name = "output.csv"

    # Download the gzip file
    os.system(f"wget {url} -O {archive_name}")

    # Extract the .gz file and rename it
    with gzip.open(archive_name, 'rb') as f_in:
        with open(csv_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Connect to PostgreSQL
    engine = create_engine(f"postgresql://{user}:{pw}@{host}:{port}/{db}")

    # Read a sample to determine data types
    df = pd.read_csv(csv_name, nrows=1)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(0).to_sql(name=table, con=engine, if_exists='replace')

    # Insert data in chunks
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    while True:
        t_start = time.time()
        try:
            df = next(df_iter)
        except StopIteration:
            break

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table, con=engine, if_exists='append')

        t_end = time.time()
        print(f'Inserted another chunk, took {t_end - t_start:.3f} seconds')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Ingest CSV data to Postgres")
    parser.add_argument('--user', help='pg username')
    parser.add_argument('--password', help='pg password')
    parser.add_argument('--port', help='pg port')
    parser.add_argument('--host', help='pg host')
    parser.add_argument('--database', help='pg database name')
    parser.add_argument('--table', help='pg table name')
    parser.add_argument('--csv_url', help='csv url')

    args = parser.parse_args()
    main(args)
