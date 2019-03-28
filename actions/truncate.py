from airflow.contrib.operators.vertica_operator import VerticaOperator
from ..sql import TRUNCATE


class TruncateVerticaOperator(VerticaOperator):
    """Truncates a table.

    Args:
        task_id: Task ID for Airflow Operator.
        vertica_conn_id: Connection ID for Vertica.

        target: Table to truncate.

    Note:
        For large tables, better use ``DeleteVerticaOperator`` with ``direct=True``.
    """

    def __init__(self, target, *args, **kwargs):
        params = dict(target=target)
        super().__init__(sql=TRUNCATE, params=params, *args, **kwargs)
