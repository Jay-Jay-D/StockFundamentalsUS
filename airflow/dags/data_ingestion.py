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

schemas = {"companies": {}}


def format_to_parquet(raw_data_folder):
    """
    Reads CSV files in raw data folder and saves it in parquet format
    """
    for csv_file in Path(raw_data_folder).glob("*.csv"):
        print(f"Processing {csv_file}")
        df = pd.read_csv(csv_file.resolve())
        # rename columns in case its name is the year. BQ is cranky with names starting with number.
        mapper=dict([(c, f'year_{c}') for c in df.columns if c[0].isdigit()])
        if any(mapper):
            df = df.rename(mapper=mapper, axis=1)
        destination_file = str(csv_file.resolve()).replace(".csv", ".parquet")
        df.to_parquet(destination_file)
        print(f"Stored {df.shape[0]} rows as {destination_file}")


def upload_to_gcs(bucket, raw_data_folder):
    """
    Uploads the parquet files from raw data folder to the data lake.
    """

    os.environ["GOOGLE_CLOUD_PROJECT"] = PROJECT_ID
    client = storage.Client()
    bucket = client.bucket(bucket)
    for parquet_file in Path(raw_data_folder).glob("*.parquet"):
        object_name = f"raw/{parquet_file.name}"
        blob = bucket.blob(object_name)
        blob.upload_from_filename(parquet_file)


def generate_bq_table_resource(table):
    pass


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
        bash_command=f"wget -v {url} -O {compressed_raw_data_path.resolve()}",
    )

    unzip_dataset_task = BashOperator(
        task_id="unzip_dataset_task",
        bash_command=f"unzip -o {compressed_raw_data_path.resolve()} -d {raw_data_folder}",
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
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"raw-companies",
            },
            "schema": {
                "fields": [
                    {
                        "name": "company_id",
                        "type": "INT64",
                        "description": "Unique company ID",
                    },
                    {
                        "name": "name_latest",
                        "type": "STRING",
                        "description": "Company's actual name",
                    },
                    {
                        "name": "name_previous",
                        "type": "STRING",
                        "description": "Company's previous names",
                    },
                ]
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/companies.parquet"],
            },
        },
    )

    gcs_2_bq_indicators_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATASET}_external_indicators_by_company_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"raw-indicators_by_company",
            },
            "schema": {
                "fields": [
                    {
                        "name": "company_id",
                        "type": "INT64",
                        "description": "Unique company ID",
                    },
                    {
                        "name": "indicator_id",
                        "type": "STRING",
                        "description": "Unique indicator ID",
                    },
                    {
                        "name": "year_2010",
                        "type": "FLOAT64",
                        "description": "2010 values",
                    },
                    {
                        "name": "year_2011",
                        "type": "FLOAT64",
                        "description": "2011 values",
                    },
                    {
                        "name": "year_2012",
                        "type": "FLOAT64",
                        "description": "2012 values",
                    },
                    {
                        "name": "year_2013",
                        "type": "FLOAT64",
                        "description": "2013 values",
                    },
                    {
                        "name": "year_2014",
                        "type": "FLOAT64",
                        "description": "2014 values",
                    },
                    {
                        "name": "year_2015",
                        "type": "FLOAT64",
                        "description": "2015 values",
                    },
                    {
                        "name": "year_2016",
                        "type": "FLOAT64",
                        "description": "2016 values",
                    },
                ]
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/indicators_by_company.parquet"],
            },
        },
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
