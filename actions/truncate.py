from airflow.contrib.operators.vertica_operator import VerticaOperator
from vertica_sql_plugin.sql import TRUNCATE


class TruncateVerticaOperator(VerticaOperator):

    def __init__(self, target, *args, **kwargs):
        params = dict(target=target)
        super().__init__(sql=TRUNCATE, params=params, *args, **kwargs)
