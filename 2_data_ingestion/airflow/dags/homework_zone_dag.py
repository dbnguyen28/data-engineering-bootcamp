import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'for_hire_vehicles')

dataset_file = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
#'fhv_tripdata_2019-01.parquet'
               
dataset_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' + dataset_file
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5MB
    storage.blob._DEFAULT_CHUCKSIZE = 5 * 1024 * 1024 # 5MB

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file, timeout=3600)
    logging.info("Successfully uploaded file to %s", object_name)

default_args = {
    "owner" : "airflow",
    "start_date" : datetime(2019, 1, 1),
    "end_date": datetime(2019, 3, 1),
    "retries": 1
}

with DAG(
    dag_id = "homework_dag",
    schedule_interval = "@monthly",
    default_args = default_args,
    max_active_runs = 1,
    tags=['hmwk']

) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command = f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket":BUCKET,
            "object_name":f"raw/{dataset_file}",
            "local_file":f"{path_to_local_home}/{dataset_file}"
        },
        execution_timeout=timedelta(minutes=30)
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "test",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{dataset_file}"],
            },
        },
    )

download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task
