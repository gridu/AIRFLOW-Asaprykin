from datetime import datetime
import logging as log
import uuid

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.postgresql_count_row import PostgreSQLCountRows

config = {
    'dag_id_4': {'owner': 'Andrey Saprykin',
                 'start_date': datetime(2018, 11, 11),
                 'table_name': 'customer'},
    'dag_id_5': {'owner': 'Andrey Saprykin',
                 'schedule_interval': "* * * * *",
                 'start_date': datetime(2019, 11, 11),
                 'table_name': 'employee'},
    'dag_id_6': {'owner': 'Andrey Saprykin',
                 'schedule_interval': "@hourly",
                 'start_date': datetime(2020, 2, 28),
                 'table_name': 'parent'}
}

database = 'airflow'
schema = 'public'
isExistDatabase = True
hook = PostgresHook()

for val in config:
    conf = config.get(val)
    table_name = conf.get('table_name')

    with DAG(val, default_args=conf, schedule_interval=conf.get('schedule_interval', None)) as dag:
        def pythonOperationCallable(table):
            return val + ' start processing table : ' + table + ', in database : ' + database

        def sendTimeToXCom(table):
            return hook.get_first(f"SELECT COUNT(*) FROM {table};")

        def isExistDatabase(table_query):
            query = hook.get_first(sql=table_query)
            log.info(query)
            return "ExistOperator" if query else "DoesNotExistOperator"

        print_start_process = PythonOperator(
            task_id='DatabaseConnection',
            python_callable=pythonOperationCallable,
            op_kwargs={'table': table_name}
        )

        print_user = BashOperator(
            task_id='PrintUser',
            bash_command="id -u -n",
            xcom_push=True
        )

        branch_database_exist_operator = BranchPythonOperator(
            task_id="CheckDatabaseExist",
            python_callable=isExistDatabase,
            op_kwargs={
                'table_query': f"SELECT * FROM information_schema.tables "
                               f"WHERE table_schema = '{schema}'"
                               f"AND table_name = '{table_name}'"},
        )

        exist_operator = DummyOperator(task_id='ExistOperator')
        does_not_exist_operator = PostgresOperator(
            task_id='DoesNotExistOperator',
            sql=f"CREATE TABLE {table_name }(custom_id integer NOT NULL,"
                "user_name VARCHAR (50) NOT NULL, timestamp TIMESTAMP NOT NULL);"
        )

        insert_new_row = PostgresOperator(
            task_id='InsertNewRow',
            trigger_rule='all_done',
            sql=f"""
                INSERT INTO {table_name} VALUES(
                {uuid.uuid4().int & (1<<16)-1},
                '{{{{task_instance.xcom_pull(task_ids='PrintUser')}}}}',
                CURRENT_TIMESTAMP 
                )
            """
        )

        query_the_table = PostgreSQLCountRows(
            task_id='QueryTheTable',
            table_name=table_name
        )

        print_start_process >> print_user >> branch_database_exist_operator >>\
        [exist_operator, does_not_exist_operator]

        exist_operator >> insert_new_row >> query_the_table
        does_not_exist_operator >> insert_new_row >> query_the_table

        globals()[val] = dag
