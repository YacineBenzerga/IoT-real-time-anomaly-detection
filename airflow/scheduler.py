import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator


schedule_interval = timedelta(days=1)

default_args = {
    'owner': 'YacineBenz',
    'depends_on_past': False,
    'start_date': datetime.now() - schedule_interval,
    'email': ['yac.benzerga@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

dag = DAG(
    'batch_scheduler',
    default_args=default_args,
    description='DAG: Spark batch Job',
    schedule_interval=schedule_interval)


task = BashOperator(
    task_id='run_batch_job',
    bash_command='cd /home/ubuntu/DataNode/bash_scripts ; ./spark_batch_run.sh',
    dag=dag)
