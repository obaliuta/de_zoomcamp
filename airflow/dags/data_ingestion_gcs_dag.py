import os
import logging
from datetime import timedelta  # <-- Add this import

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import TaskInstance

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# Environment variables (set these appropriately in Airflow)
PROJECT_ID = os.environ.get("PROJECT_ID", "zoomcamp-455010")
BUCKET = os.environ.get("GCS_BUCKET", "trips_raw_data")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# Dataset details
dataset_file = "yellow_tripdata_2021-01.parquet"
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file  # already in parquet now -- .replace('.csv', '.parquet')


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file, **kwargs):
    """
    Uploads a file to Google Cloud Storage (GCS) and pushes a success message to XCom.
    
    Args:
        bucket (str): GCS bucket name.
        object_name (str): Destination object path in the bucket.
        local_file (str): Local file path to upload.
        **kwargs: Airflow context args, required for XCom push.

    Raises:
        FileNotFoundError: If the local file does not exist.
    """
    if not os.path.exists(local_file):
        raise FileNotFoundError(f"Local file '{local_file}' not found!")

    # WORKAROUND to prevent timeout for large files on slow upload connections
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024   # 5 MB

    logging.info(f"Uploading {local_file} to gs://{bucket}/{object_name}")

    client = storage.Client()
    gcs_bucket = client.bucket(bucket)
    blob = gcs_bucket.blob(object_name)

    # Upload the file to GCS
    blob.upload_from_filename(local_file)

    logging.info(f"Upload completed: {local_file} → gs://{bucket}/{object_name}")

    # Push a success message to XCom
    ti: TaskInstance = kwargs['ti']
    success_message = f"File {local_file} uploaded successfully to gs://{bucket}/{object_name}"
    ti.xcom_push(key='upload_status', value=success_message)


def process_upload_status(**kwargs):
    """
    Pulls the status of the file upload from XCom and logs it.
    """
    ti: TaskInstance = kwargs['ti']
    upload_status = ti.xcom_pull(task_ids='local_to_gcs_task', key='upload_status')
    
    if upload_status:
        logging.info(f"Upload status: {upload_status}")
    else:
        logging.error("No upload status found in XCom!")


# Default arguments for DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# DAG definition using Context Manager
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",  # Daily schedule
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    # Download dataset task
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    # Task to upload local file to GCS
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
        provide_context=True,  # Make sure context is passed for XCom push
    )

    # New task to process and log the upload status from XCom
    log_upload_status_task = PythonOperator(
        task_id="log_upload_status_task",
        python_callable=process_upload_status,
        provide_context=True,  # Make sure context is passed for XCom pull
    )

    # BigQuery external table creation task
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
            },
        },
    )

    # Set task dependencies
    download_dataset_task >> local_to_gcs_task >> log_upload_status_task >> bigquery_external_table_task
