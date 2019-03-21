from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from vertica_sql_plugin.sql import INSERT, GET_TABLE_COLUMNS


class InsertVerticaOperator(VerticaOperator):

    def __init__(self, source, target, date_column=None, direct=False, column_mapping=None, force_introspection=False,
                 exclude=None,
                 *args, **kwargs):
        if column_mapping is None or force_introspection:
            exclude = exclude or []
            vertica_conn_id = kwargs.get('vertica_conn_id', 'vertica_default')
            hook = VerticaHook(vertica_conn_id=vertica_conn_id)
            columns = {c: c for (c,) in hook.get_records(GET_TABLE_COLUMNS, dict(table=target)) if c not in exclude}
        column_mapping = column_mapping or {}
        columns.update(column_mapping)
        source_columns, target_columns = list(zip(*columns.items()))
        params = dict(source=source, target=target, date_column=date_column, direct=direct,
                      source_columns=', '.join(source_columns), target_columns=', '.join(target_columns))
        super().__init__(sql=INSERT, params=params, *args, **kwargs)
