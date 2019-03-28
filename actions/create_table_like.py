from airflow.contrib.operators.vertica_operator import VerticaOperator
from vertica_sql_plugin.sql import CREATE_TABLE_LIKE


class CreateTableLikeVerticaOperator(VerticaOperator):

    def __init__(self, target, source, projections=False, *args, **kwargs):
        """
        Args:
            target:
            source:
            projections:
            *args:
            **kwargs:
        """
        params = dict(target=target, source=source, projections=projections)
        super().__init__(sql=CREATE_TABLE_LIKE, params=params, *args, **kwargs)
