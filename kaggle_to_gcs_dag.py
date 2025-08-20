# airflow/dags/kaggle_to_gcs_dag.py
import sys
sys.path.append("/opt/airflow/scripts")  # so DAG can import local scripts

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from load_kaggle_to_gcs import clean_and_upload  # assuming function is named clean_and_upload

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "kaggle_to_gcs_dag",
    default_args=default_args,
    description="Ingest multiple Kaggle traffic CSVs into GCS raw/traffic/",
    schedule_interval=None,   # manual trigger for now
    start_date=datetime(2025, 7, 1),
    catchup=False,
    tags=["kaggle", "gcs", "batch"],
) as dag:

    accident_task = PythonOperator(
        task_id="upload_accident",
        python_callable=clean_and_upload,
        op_kwargs={
            "csv_path": "/opt/airflow/data/accident.csv",
            "dataset_type": "accident",
        },
    )

    casualty_task = PythonOperator(
        task_id="upload_casualty",
        python_callable=clean_and_upload,
        op_kwargs={
            "csv_path": "/opt/airflow/data/casualty.csv",
            "dataset_type": "casualty",
        },
    )

    vehicle_task = PythonOperator(
        task_id="upload_vehicle",
        python_callable=clean_and_upload,
        op_kwargs={
            "csv_path": "/opt/airflow/data/vehicle.csv",
            "dataset_type": "vehicle",
        },
    )

    # Run all in parallel (or chain them with >> if needed)
    [accident_task, casualty_task, vehicle_task]
