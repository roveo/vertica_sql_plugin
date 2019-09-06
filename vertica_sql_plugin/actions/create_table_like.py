from airflow.contrib.operators.vertica_operator import VerticaOperator
from vertica_sql_plugin.sql import CREATE_TABLE_LIKE


class CreateTableLikeVerticaOperator(VerticaOperator):
    """Creates a new empty table that's a copy of an existing one.

    Args:
        task_id: Task ID for Airflow Operator.
        vertica_conn_id: Connection ID for Vertica.

        target: Which table to create.
        source: Which table to use as source.
        projections (bool): Defaults to ``False``.

            Whether to include Vertica ``INCLUDING PROJECTIONS`` directive.
    """


    def __init__(self, target, source, projections=False, *args, **kwargs):
        params = dict(target=target, source=source, projections=projections)
        super().__init__(sql=CREATE_TABLE_LIKE, params=params, *args, **kwargs)
