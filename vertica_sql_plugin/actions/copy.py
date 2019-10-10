from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from vertica_sql_plugin.sql import COPY

import logging

logger = logging.getLogger(__name__)



class CopyFromStdinVerticaOperator(VerticaOperator):
    """Copies CSV into Vertica from a file-like object.

    Use this for simple copying of csv data without saving the data on disk, for example, you can steam a file
    directly from a http source with the ``requests`` library.

    Args:
        target: Table to copy to.
        source: Callable that will return an object with a ``.read()`` method.

        sql: Optional SQL query to override the default.
        delimiter: Defaults to ``','``. CSV delimiter.
        skip: Default: 0. Vertica ``COPY`` statement option.
        direct: Default: False. Vertica ``COPY`` statement option.
        trailing_nullcols: Default: ``False``. Vertica ``COPY`` statement option.
        enforcelength: Default: ``True``. Vertica ``COPY`` statement option.
        rejected_data_as_table: Default: ``''``. Vertica ``COPY`` statement option.
        truncate: Default: ``False``. Truncate target table before COPY;
    """

    def __init__(self, target, source, sql=None, delimiter=',', skip=0, direct=False, trailing_nullcols=False,
                 enforcelength=True, abort_on_error=True, compression='', parser='', rejected_data_as_table='',
                 enclosed_by='"', truncate=False, *args, **kwargs):
        self.source = source
        sql = sql or COPY
        params = dict(target=target, delimiter=delimiter, skip=skip, direct=direct, trailing_nullcols=trailing_nullcols,
                      enforcelength=enforcelength, abort_on_error=abort_on_error, compression=compression, parser=parser,
                      rejected_data_as_table=rejected_data_as_table, enclosed_by=enclosed_by, truncate=truncate)
        super().__init__(sql=sql, params=params, *args, **kwargs)

    def execute(self, context=None):
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                logging.info(self.sql)
                cur.copy(self.sql, self.source(**context))
