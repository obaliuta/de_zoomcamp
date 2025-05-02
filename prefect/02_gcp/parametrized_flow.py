from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(retries=3, retry_delay_seconds=5) #, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read large taxi data in chunks and concatenate"""
    chunks = []
    for chunk in pd.read_csv(dataset_url, compression="gzip", low_memory=False, chunksize=100_000):
        chunks.append(chunk)
    df = pd.concat(chunks, ignore_index=True)
    print(df.columns)
    return df

@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues for large DataFrames"""
    # Convert in chunks to avoid high memory spikes
    df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
    df['dropOff_datetime'] = pd.to_datetime(df['dropOff_datetime'], errors='coerce')

    # Log sample and metadata, not full data
    print(df.iloc[:2].to_string(index=False))
    print("columns:", df.dtypes.to_dict())
    print(f"row count: {len(df):,}")

    # Optional: Force garbage collection if running in a memory-sensitive environment
    import gc; gc.collect()

    return df

@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out as parquet file - local save"""
    os.makedirs(f"data/{color}", exist_ok=True)
    path = Path(f"data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path

@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("tripsrawdatabucket")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path,
    )
    os.remove(path)
    print(f"Uploaded {path} to GCS and removed local copy.")

@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
        f"{color}/{dataset_file}.csv.gz"
    )
    df = fetch(dataset_url)
    df_clean = clean(df)
    path = write_local(df_clean, color, dataset_file)
    write_gcs(path)

@flow()
def etl_parent_flow(
    color: str = 'yellow', year: int = 2021, months: list[int] = [1, 2, 3]) -> None:
    for month in months:
        etl_web_to_gcs(color, year, month)

if __name__ == "__main__":
    color = 'fhv'
    months = [1]
    year = 2019
    etl_parent_flow(color=color, year=year, months=months)
