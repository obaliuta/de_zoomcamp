from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os

@task(log_prints=True)
def fetch_and_write_local(dataset_url: str, color: str, dataset_file: str) -> list[Path]:
    """Stream data in chunks and write each as a separate Parquet file"""
    dir_path = Path(f"data/{color}/{dataset_file}")
    dir_path.mkdir(parents=True, exist_ok=True)
    paths = []

    for i, chunk in enumerate(pd.read_csv(dataset_url, compression="gzip", low_memory=False, chunksize=100_000)):
        chunk.tpep_pickup_datetime = pd.to_datetime(chunk.tpep_pickup_datetime)
        chunk.tpep_dropoff_datetime = pd.to_datetime(chunk.tpep_dropoff_datetime)
        chunk_path = dir_path / f"{dataset_file}_part{i}.parquet"
        chunk.to_parquet(chunk_path, compression="gzip", engine="pyarrow")
        paths.append(chunk_path)
        print(f"Wrote chunk {i} to {chunk_path}")

    return paths


@task()
def write_gcs(paths: list[Path]) -> None:
    gcs_block = GcsBucket.load("tripsrawdatabucket")
    for path in paths:
        gcs_block.upload_from_path(from_path=path, to_path=path)
        os.remove(path)
        print(f"Uploaded {path} and removed local copy.")


@flow()
def etl_web_to_gcs(
    color: str = "yellow",
    year: int = 2021,
    month: int = 1,
    gcs_block_name: str = "gcs-bucket-block",
):
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = (
        f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"
        f"{color}/{dataset_file}.csv.gz"
    )

    # ← Pass the URL, not the filename
    path = fetch_and_write_local(dataset_url, color, dataset_file)
    write_gcs(path)


if __name__ == "__main__":
    etl_web_to_gcs()
