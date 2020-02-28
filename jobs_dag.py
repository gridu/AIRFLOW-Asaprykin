from datetime import datetime
from airflow import DAG

config = {
    'dag_id_7': {'owner': 'Andrey Saprykin', 'schedule_interval': None, 'start_date': datetime(2018, 11, 11)},
    'dag_id_8': {'owner': 'Andrey Saprykin', 'schedule_interval': "* * * * *", 'start_date': datetime(2019, 11, 11)},
    'dag_id_9': {'owner': 'Andrey Saprykin', 'schedule_interval': "@hourly", 'start_date': datetime(2020, 2, 28)}
}

for val in config:
    with DAG(val, default_args=config.get(val)) as dag:
        globals()[val] = dag
