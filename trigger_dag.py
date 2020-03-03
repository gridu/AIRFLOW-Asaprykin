from datetime import datetime

from airflow import DAG
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python_operator import PythonOperator

default_attributes = {
    "owner": "Andrey Saprykin",
    "start_date": datetime(2018, 1, 1)
}

folder = Variable.get('PathToRunFile', '')
external_dag_id = "dag_id_4"
docker_path = "/usr/local/airflow/dags"

with DAG("sensor_dag", default_args=default_attributes, schedule_interval=None) as dag:
    def create_sub_dag(parent_dag_name, child_dag_name, start_date, schedule_interval=None):
        name = parent_dag_name + "." + child_dag_name

        sub_dag = DAG(name, schedule_interval=schedule_interval, start_date=start_date)

        def print_result(**context):
            return context['task_instance'].xcom_pull(task_ids='QueryTheTable', dag_id=external_dag_id, key=None)

        external_task_sensor = ExternalTaskSensor(
            task_id="ExternalTaskSensor",
            dag=sub_dag,
            external_dag_id=external_dag_id,
            poke_interval=5
        )

        print_result_external_task = PythonOperator(
            task_id="GetXComInformation",
            dag=sub_dag,
            python_callable=print_result,
            provide_context=True
        )

        bash_remove_operator = BashOperator(
            task_id="RemoveFolder",
            dag=sub_dag,
            bash_command=f"rm {docker_path}/{folder}/run"
        )

        create_file = BashOperator(
            task_id="CreateFile",
            dag=sub_dag,
            bash_command=f"touch {docker_path}/{folder}/finished_{{{{ ts_nodash }}}}"
        )

        external_task_sensor >> \
            print_result_external_task >> \
            bash_remove_operator >> \
            create_file

        return sub_dag

    file_sensor = FileSensor(
        task_id="FileSensor",
        poke_interval=3,
        filepath=f"{folder}/run"
    )

    trigger_dag_operator = TriggerDagRunOperator(
        task_id="TriggerDagRun",
        trigger_dag_id=external_dag_id,
        execution_date="{{ execution_date }}"
    )

    sub_dag_operator = SubDagOperator(
        task_id="SubDag",
        subdag=create_sub_dag(dag.dag_id, "SubDag", default_attributes.get("start_date"))
    )

    file_sensor >> trigger_dag_operator >> sub_dag_operator
