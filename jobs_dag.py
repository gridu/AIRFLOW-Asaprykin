from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

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

for val in config:

    conf = config.get(val)

    with DAG(val, default_args=conf, schedule_interval=conf.get('schedule_interval', None)) as dag:

        def pythonOperationCallable(table):
            return val + ' start processing table : ' + table + ', in database : ' + database

        t1 = PythonOperator(
            task_id='DatabaseConnection',
            python_callable=pythonOperationCallable,
            op_kwargs={'table': conf.get('table_name')}
        )
        insert_new_row = DummyOperator(task_id='InsertNewRow')
        query_the_table = DummyOperator(task_id='QueryTheTable')

        t1 >> insert_new_row >> query_the_table
        globals()[val] = dag
