from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from etl.realtime_extract import extract_realtime_data
from etl.realtime_transform import transform_realtime_data
from etl.realtime_load import load_realtime_data

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "daily_realtime_etl",
    default_args=default_args,
    description="Daily real-time ETL for US air quality data",
    schedule_interval="59 23 * * *",
    catchup=False,
)

extract = PythonOperator(
    task_id="extract_realtime_data", python_callable=extract_realtime_data, dag=dag
)

transform = PythonOperator(
    task_id="transform_realtime_data", python_callable=transform_realtime_data, dag=dag
)

load = PythonOperator(
    task_id="load_realtime_data", python_callable=load_realtime_data, dag=dag
)

extract >> transform >> load
