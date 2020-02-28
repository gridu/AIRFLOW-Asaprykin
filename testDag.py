from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'Andrey Saprykin',
    'start_date': datetime(2018, 1, 1)
}

with DAG('gridu_dag', default_args=default_args, schedule_interval='@once') as dag:
    BashOperator(
        task_id='print_date',
        bash_command='date',
        schedule_interval=timedelta(days=1)
    )

print("hello log file")
