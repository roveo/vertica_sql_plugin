from airflow.contrib.operators.vertica_operator import VerticaOperator
from vertica_sql_plugin.sql import DELETE


class DeleteVerticaOperator(VerticaOperator):

    def __init__(self, target, date_column=None, direct=False, *args, **kwargs):
        params = dict(target=target, date_column=date_column, direct=direct)
        super().__init__(sql=DELETE, params=params, *args, **kwargs)
