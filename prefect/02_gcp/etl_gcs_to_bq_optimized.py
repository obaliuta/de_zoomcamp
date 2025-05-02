from pathlib import Path
import pandas as pd
import argparse
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials
import logging

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
    """Transform the data, replacing invalid or missing values with 0s."""
    try:
        df = pd.read_parquet(path)
    except Exception as e:
        logging.error(f"Failed to read parquet file at {path}: {e}")
        return pd.DataFrame()  # Or raise if preferred

    # Columns to clean
    int_columns = ['PULocationID', 'DOLocationID', 'SR_Flag']

    for col in int_columns:
        if col in df.columns:
            try:
                # Replace missing or NaN values with 0
                df[col] = df[col].fillna(0)
                # Coerce any problematic values (e.g., strings) to numeric, replacing invalid entries with 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
            except Exception as e:
                logging.warning(f"Failed to clean column {col}: {e}")
                df[col] = 0  # fallback to zero for entire column if serious issue

    # Filter out rows with 0 passengers
    #df = df[df['passenger_count'] != 0]

    return df 

@task()
def write_bq(df: pd.DataFrame, color: str):
    """Write the DataFrame to BigQuery."""
    gcp_credentials_block = GcpCredentials.load("gcpcredentials")
    df.to_gbq(
        destination_table=f"trips_data_all.int_{color}_rides",
        project_id="zoomcamp-455010",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )

@flow()
def etl_gcs_to_bq(year: int, months: list[int], dataset: str = "trips_data_all"):
    """
    ETL flow to extract data for multiple months from GCS, transform, and load into BigQuery.
    """
    color = 'fhv'
    paths = extract_from_gcs.map(
        [color] * len(months),
        [year] * len(months),
        months
    )
    dfs = transform.map(paths)
    write_bq.map(dfs, [color] * len(dfs))

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL GCS to BigQuery for multiple months.")
    parser.add_argument('year', type=int, help='Year for the trip data')
    parser.add_argument('months', type=int, nargs='+', help='List of months to process (e.g., 1 2 3)')
    parser.add_argument('--dataset', type=str, default='trips_data_all', help='Target BigQuery dataset')
    args = parser.parse_args()

    etl_gcs_to_bq(year=args.year, months=args.months, dataset=args.dataset)

    
#python etl_gcs_to_bq_optimized.py 2019 1 2 3 4
#above will cause performance issue - use a shell script run_etl_sequential.sh to loop through months
