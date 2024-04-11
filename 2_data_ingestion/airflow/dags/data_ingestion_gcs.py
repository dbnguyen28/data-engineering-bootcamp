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

#dataset_file = 'yellow_tripdata_2021-01.parquet'
dataset_file = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'

dataset_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' + dataset_file
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
#parquet_file = dataset_file.replace('.csv', '.parquet')
parquet_file = dataset_file
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

def format_to_parquet(src_file):
    if src_file.endswith('.parquet'):
        logging.warning("source file is already a parquet")
    elif not src_file.endswith('.csv'):
        logging.error("Can only accept source files in csv format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))

def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)

    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024 # 5MB
    storage.blob._DEFAULT_CHUCKSIZE = 5 * 1024 * 1024 # 5MB
    # End of workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)
    logging.info("THIS IS A TEST ################################################################")
    logging.info("Successfully uploaded file to %s", object_name)

default_args = {
    "owner" : "airflow",
    "depends_on_past":False,
    "retries": 1
}

# Note: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date = datetime(2019, 12, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    tags=['dtc-de']
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{path_to_local_home}/{dataset_file}",
    #     },
    # )

    #TODO: homework -research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET, 
            "object_name": f"raw/{parquet_file}", 
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

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


download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task