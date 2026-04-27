# dags/hello_pipeline.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("ETL pipeline is alive!")

with DAG(
    dag_id="hello_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )