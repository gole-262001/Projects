from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from spark_code import spark_transform
from hive_code import spark_hive_store


default_args={
        'owner':'Abhishek',
        'email' :['abhishekgole747@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries':3,
        'retry_delay':timedelta(minutes=5)
        }

covid19= DAG(
        'covid',
        default_args=default_args,
        description='covid19Analysis',
        start_date=datetime(2023, 4 ,5),
        schedule_interval='* * * * *',
        catchup=False,
        tags=['example,covid19project']
        )


# Task 1: Dummy Operator to start the task
task1 = DummyOperator(task_id='start_task', dag=covid19)

spark_code = PythonOperator(
        task_id='run_spark_job',
        python_callable=spark_transform,
        dag=covid19)

# Task 3: Dummy Operator to end the task
task3 = DummyOperator(task_id='end_task', dag=covid19)

hive_code = PythonOperator(
        task_id='run_hive_storage_process',
        python_callable=spark_hive_store,
        dag=covid19)

# Define task dependenciesi
task1 >> spark_code >> hive_code >> task3
