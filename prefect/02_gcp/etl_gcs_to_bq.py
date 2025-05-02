from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
from prefect_gcp.bigquery import BigQueryWarehouse


@task()
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download a single file from GCS."""
    file_name = f"{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = f"data/{color}/{file_name}"
    local_dir = Path(f"data/{color}")
    local_dir.mkdir(parents=True, exist_ok=True)
    local_path = local_dir / file_name
    gcs_block = GcsBucket.load("tripsrawdatabucket")
    gcs_block.download_object_to_path(from_path=gcs_path, to_path=local_path)

    return local_path


@task(retries=3, retry_delay_seconds=5)
def transform(path: Path) -> pd.DataFrame:
    """Transform the data."""
    df = pd.read_parquet(path)
    df['passenger_count'] = df['passenger_count'].fillna(0)
    print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    df = df[df['passenger_count'] != 0]
    print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
    return df    

@task()
def write_bq(df: pd.DataFrame):
    """Write the DataFrame to BigQuery."""

    gcp_credentials_block = GcpCredentials.load("gcpcredentials")

    df.to_gbq(
        destination_table="trips_data_all.rides",
        project_id="zoomcamp-455010",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq(year: int, month: int, dataset: str):
    """
    ETL flow to extract data from GCS, transform it, and load it into BigQuery.
    """
    color = 'yellow'
    path = extract_from_gcs(color, year, month)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    # Example call with default params — change as needed
    etl_gcs_to_bq(year=2021, month=1, dataset="trips_data_all")
