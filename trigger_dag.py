from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_attributes = {
    "owner": "Andrey Saprykin",
    "start_date": datetime(2018, 1, 1)
}

with DAG("sensor_dag", default_args=default_attributes, schedule_interval=None) as dag:

    file_sensor = FileSensor(
        task_id="FileSensor",
        poke_interval=3,
        filepath="run"
    )

    trigger_dag_operator = TriggerDagRunOperator(
        task_id="TriggerDagRun",
        trigger_dag_id="dag_id_4"
    )

    bash_operator = BashOperator(
        task_id="RemoveFolder",
        bash_command="rm -rf folder/run"
    )

    file_sensor >> trigger_dag_operator >> bash_operator