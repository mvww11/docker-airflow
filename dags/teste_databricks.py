"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
import airflow
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.contrib.operators.databricks_operator import DatabricksRunNowOperator
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


with DAG('databricks_dag',
    start_date=datetime(2021, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    json = {
    "job_id": 41339
    }

    t1 = DatabricksRunNowOperator(
        task_id='notebook_run',
        databricks_conn_id="databricks_default",
        job_id=41339,
        dag=dag
    )

    t2 = DummyOperator(task_id='B', dag=dag)

    t1 >> t2