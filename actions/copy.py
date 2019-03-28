from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from vertica_sql_plugin.sql import COPY


class CopyFromStdinVerticaOperator(VerticaOperator):

    def __init__(self, target, source, sql=None, delimiter=',', skip=0, direct=False, trailing_nullcols=False,
                 enforcelength=True, abort_on_error=True, compression='', parser='', *args, **kwargs):
        """
        Args:
            target:
            source:
            sql:
            delimiter:
            skip:
            direct:
            trailing_nullcols:
            enforcelength:
            abort_on_error:
            compression:
            parser:
            *args:
            **kwargs:
        """
        self.source = source
        sql = sql or COPY
        params = dict(target=target, delimiter=delimiter, skip=skip, direct=direct, trailing_nullcols=trailing_nullcols,
                      enforcelength=enforcelength, abort_on_error=abort_on_error, compression=compression, parser=parser)
        super().__init__(sql=sql, params=params, *args, **kwargs)

    def execute(self, context=None):
        """
        Args:
            context:
        """
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.copy(self.sql, self.source(context=context))
