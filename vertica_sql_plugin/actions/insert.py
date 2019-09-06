from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from vertica_sql_plugin.sql import INSERT, GET_TABLE_COLUMNS


class InsertVerticaOperator(VerticaOperator):
    r"""Inserts data from source table or view into target table.

    If ``column_mapping`` is not provided by the user, it will run an inspection on the target table to list it's
    columns. Column names are expected to be the same in the source. Note that the inspection is run when the DAG
    is created, not when it's run, so be aware of how much strain it puts on the database.

    Args:
        task_id: Task ID for Airflow Operator.
        vertica_conn_id: Connection ID for Vertica.

        source: Source table or view.
        target: Target table.

        date_column: Defaults to ``None``.

            If provided, the source will be additionally filtered on this column
            to be ``>= execution_date and < next_execution_date``.

        truncate_date (bool): Defaults to ``None``.

            If True, ``execution_date`` will be truncated to date.
            This is useful when your ``start_date`` is in the morning but the processed data
            should be between 00:00 and 00:00 next day.

        direct (bool): Defaults to ``False``.

            Will add a ``/* +direct */`` Vertica directive to the INSERT statement.

        column_mapping (dict): Defaults to ``None``.

            A mapping from target to source columns. If provided, the operator won't run database introspection
            to infer these automatically, so only mapped columns will be inserted.

        force_introspection (bool): Defaults to ``False``.

            If provided together with ``column_mapping``, will force the database introspection anyway and merge
            the user-provided mapping into the result of introspection. Use this when most column names in source
            and target are the same, but some aren't.

        exclude (list): Defaults to ``None``.

            List of columns to not select from the source. Use this for columns created with a ``default`` option.
    """

    def __init__(self, source, target, date_column=None, truncate_date=False,
                 direct=False, column_mapping=None, force_introspection=False, exclude=None, sql=None, *args, **kwargs):
        if column_mapping is None or force_introspection:
            exclude = exclude or []
            vertica_conn_id = kwargs.get('vertica_conn_id', 'vertica_default')
            hook = VerticaHook(vertica_conn_id=vertica_conn_id)
            columns = {c: c for (c,) in hook.get_records(GET_TABLE_COLUMNS, dict(table=target)) if c not in exclude}
            self.log.debug('Target columns discovered: {}'.format(list(columns)))
        column_mapping = column_mapping or {}
        columns.update(column_mapping)
        source_columns, target_columns = list(zip(*columns.items()))
        _sql = sql or INSERT
        params = dict(source=source, target=target, date_column=date_column, truncate_date=truncate_date, direct=direct,
                      source_columns=', '.join(source_columns), target_columns=', '.join(target_columns))
        super().__init__(sql=_sql, params=params, *args, **kwargs)
