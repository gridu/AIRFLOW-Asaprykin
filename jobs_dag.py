from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator

config = {
    'dag_id_4': {'owner': 'Andrey Saprykin',
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'table_name_1'},
    'dag_id_5': {'owner': 'Andrey Saprykin',
                 'schedule_interval': "* * * * *",
                 'start_date': datetime(2019, 11, 11),
                 'table_name': 'table_name_2'},
    'dag_id_6': {'owner': 'Andrey Saprykin',
                 'schedule_interval': "@hourly",
                 'start_date': datetime(2020, 2, 28),
                 'table_name': 'table_name_3'}
}

database = 'someSchema'
isExistDatabase = True

for val in config:

    conf = config.get(val)

    with DAG(val, default_args=conf, schedule_interval=conf.get('schedule_interval', None)) as dag:

        def pythonOperationCallable(table, **context):
            Variable.set(key="execution_date_jobs", value=context['execution_date'])
            return val + ' start processing table : ' + table + ', in database : ' + database

        def sendTimeToXCom(**context):
            # return f"{context['run_id']} ended" # -- print only tasks id
            return f"{context}" # -- print whole context of task

        def isExistDatabase():
            return "ExistOperator" if isExistDatabase else "DoesNotExistOperator"

        print_user = BashOperator(
            task_id='PrintUser',
            bash_command='echo $USER'
        )

        print_start_process = PythonOperator(
            task_id='DatabaseConnection',
            python_callable=pythonOperationCallable,
            provide_context=True,
            op_kwargs={'table': conf.get('table_name')}
        )
        insert_new_row = DummyOperator(task_id='InsertNewRow', trigger_rule='all_done')
        query_the_table = PythonOperator(
            task_id='QueryTheTable',
            python_callable=sendTimeToXCom,
            provide_context=True
        )

        branch_database_exist_operator = BranchPythonOperator(
            task_id="CheckDatabaseExist",
            python_callable=isExistDatabase
        )
        exist_operator = DummyOperator(task_id='ExistOperator')
        does_not_exist_operator = DummyOperator(task_id='DoesNotExistOperator')

        print_start_process >> print_user >> branch_database_exist_operator >> [exist_operator, does_not_exist_operator]
        exist_operator >> insert_new_row >> query_the_table
        does_not_exist_operator >> insert_new_row >> query_the_table

        globals()[val] = dag
