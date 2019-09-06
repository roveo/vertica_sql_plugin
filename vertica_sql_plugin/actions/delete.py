from airflow.contrib.operators.vertica_operator import VerticaOperator
from vertica_sql_plugin.sql import DELETE


class DeleteVerticaOperator(VerticaOperator):
    """Deletes rows from target table.

    Args:
        task_id: Task ID for Airflow Operator.
        vertica_conn_id: Connection ID for Vertica.

        target: Table to delete the rows from.
        date_column: Defaults to ``None``.

            If provided, the ``DELETE`` statement will have a ``WHERE`` clause that's
            ``>= execution_date AND < next_execution_date``.

        truncate_date: Defaults to ``False``.

            If ``True``, will truncate ``execution_date`` to date. Useful when processing whole days of data regardless
            if actual DAG ``start_date``.

        direct: Defaults to ``False``.

            If, ``True``, sets a ``/* +direct */`` Vertica directive for the ``DELETE`` statement.
    """

    def __init__(self, target, date_column=None, truncate_date=False, direct=False, *args, **kwargs):
        params = dict(target=target, date_column=date_column, truncate_date=truncate_date, direct=direct)
        super().__init__(sql=DELETE, params=params, *args, **kwargs)
