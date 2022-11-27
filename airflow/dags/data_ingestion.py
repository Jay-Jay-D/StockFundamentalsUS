import os
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
)

from datetime import datetime, timedelta

from google.cloud import storage

import pandas as pd

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
DATASET = "stock_fundamental_us"
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "sotck_fundamentals_us")
YESTERDAY = datetime.now() - timedelta(days=1)

url = "https://query.data.world/s/vubufdyuqbjyhll3bsi3fw3dn4m4jt"
raw_data_folder = "/raw_data/"
compressed_raw_data_path = Path(raw_data_folder + "us-stocks-fundamentals.zip")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}


def format_to_parquet(raw_data_folder):
    """
    Reads CSV files in raw data folder and saves it in parquet format
    """
    for csv_file in Path(raw_data_folder).glob("*.csv"):
        print(f"Processing {csv_file}")
        df = pd.read_csv(csv_file.resolve())
        destination_file = str(csv_file.resolve()).replace(".csv", ".parquet")
        df.to_parquet(destination_file)
        print(f"Stored {df.shape[0]} rows as {destination_file}")


def upload_to_gcs(bucket, raw_data_folder):
    """
    Uploads the parquet files from raw data folder to the data lake.
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)
    for parquet_file in Path(raw_data_folder).glob("*.parquet"):
        object_name = f"raw/{parquet_file.name}"
        blob = bucket.blob(object_name)
        blob.upload_from_filename(parquet_file)


with DAG(
    dag_id="stock_fundamental_us_data_ingestion",
    schedule_interval="0 12 * * *",
    start_date=YESTERDAY,
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=["SFU-POC"],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {url} >> {compressed_raw_data_path.resolve()}",
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip {compressed_raw_data_path.resolve()} && rm -v {compressed_raw_data_path.resolve()}",
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "raw_data_folder": f"{raw_data_folder}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={"bucket": BUCKET, "raw_data_folder": f"{raw_data_folder}"},
    )

    clean_raw_data_folder_task = BashOperator(
        task_id="clean_raw_data_folder", bash_command=f"rm -v {raw_data_folder}*"
    )

    gcs_2_bq_companies_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATASET}_external_companies_table_task",
        bucket=BUCKET,
        source_objects=["/raw/companies.parquet"],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.raw.companies",
        source_format="PARQUET",
    )

    gcs_2_bq_indicators_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATASET}_external_indicators_by_company_table_task",
        bucket=BUCKET,
        source_objects=["/raw/indicators_by_company.parquet"],
        destination_project_dataset_table=f"{BIGQUERY_DATASET}.raw.indicators_by_company",
        source_format="PARQUET",
    )

    print_dag_run_conf = BashOperator(
        task_id="print_dag_run_conf", bash_command="echo {{ dag_run.id }}"
    )

    (
        download_dataset_task
        >> unzip_dataset_task
        >> format_to_parquet_task
        >> local_to_gcs_task
        >> clean_raw_data_folder_task
        >> gcs_2_bq_companies_task
        >> gcs_2_bq_indicators_task
        >> print_dag_run_conf
    )
