import datetime
import sys
import os

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

#sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, "/Volumes/ext_ssd/portfolio/stock_forecasting")

from data.downloader import download_pipeline


default_args = {
    "owner" : "airflow",
    "depends_on_past" : False,
    "email_on_failure" : False,         # to be changed
    "email_on_retry" : False,
    "retries" : 1,
    "retry_delay" : datetime.timedelta(minutes = 5)
}

with DAG(
    "daily_sotck_data_download",
    default_args = default_args,
    description = "Dowloads and stores stock data every day at 1 am",
    schedule = "0 1 * * *",
    start_date = datetime.datetime(2025, 9, 23),
    catchup = False,
    tags = ["stocks", "downloads"],
) as dag:

    run_download = PythonOperator(
        task_id = "run_download_pipeline",
        python_callable = download_pipeline
    )

    run_download