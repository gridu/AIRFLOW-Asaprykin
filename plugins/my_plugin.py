import logging as log

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


class PostgreSQLCountRows(BaseOperator):
    @apply_defaults
    def __init__(self, table_name, *args, **kwargs):
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)
        self.hook = PostgresHook()
        self.table_name = table_name

    def execute(self, context):
        result = self.hook.get_first(sql=f"SELECT COUNT(*) FROM {self.table_name}")
        log.info(result)
        return result


class AirflowTestPlugin(AirflowPlugin):
    name = "postgresql_count_row"
    operators = [PostgreSQLCountRows]