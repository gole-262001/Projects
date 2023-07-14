from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime,timedelta
from pyspark.sql import SparkSession
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.email_operator import EmailOperator

default_args={
        'owner':'Abhishek',
        'email' :['abhishekgole747@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries':3,
        'retry_delay':timedelta(minutes=5)
        }

covid19= DAG(
        'covid19',
        default_args=default_args,
        description='covid19Analysis',
        start_date=datetime(2023, 4 ,5),
        schedule_interval='* * * * *',
        catchup=False,
        tags=['example,covid19project']
        )


# Task 1: Dummy Operator to start the task
task1 = DummyOperator(task_id='start_task', dag=covid19)

# Task 2: Run Spark job to read CSV and send output file
def run_spark_job():
    import pyspark
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master("local[1]").appName("Covid 19 analysis").getOrCreate()
    CovidData = spark.read.option("header" , True).csv("/root/airflow/inputfiles/India__Covid_State_Data.csv").drop("Population")
    VaccineData = spark.read.option("header", True).csv("/root/airflow/inputfiles/India_Vaccine_Data.csv")
    joinedData = CovidData.join(VaccineData, "State/UTs")
    joinedData.write.csv('/root/airflow/outputfiles/covidDataOutput.csv', mode='overwrite', header=True)


task2 = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
        dag=covid19)

# Task 3: Dummy Operator to end the task
task3 = DummyOperator(task_id='end_task', dag=covid19)
email_task = EmailOperator(
        task_id="email",
        to=['abhishekgole747@gmail.com'],
        subject="Airflow successfull!",
        html_content="<i>Covid output data receive successfully t</i>"
        )
# Define task dependenciesi
task1 >> task2 >> task3 >>email_task