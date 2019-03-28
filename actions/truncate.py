from airflow.contrib.operators.vertica_operator import VerticaOperator
from ..sql import TRUNCATE


class TruncateVerticaOperator(VerticaOperator):
    """Truncates a table.

    Args:
        target: Table to truncate.
        vertica_conn_id: Airflow connection ID.

    Note:
        For large tables, better use ``DeleteVerticaOperator`` with ``direct=True``.
    """

    def __init__(self, target, *args, **kwargs):
        """
        Args:
            target:
            *args:
            **kwargs:
        """
        params = dict(target=target)
        super().__init__(sql=TRUNCATE, params=params, *args, **kwargs)
