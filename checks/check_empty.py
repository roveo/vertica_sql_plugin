from contextlib import closing

from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.exceptions import AirflowException
from vertica_sql_plugin.sql import (
    COUNT,
    ANALYZE_CONSTRAINTS,
    NON_UNIQUE_KEYS,
    KEYS
)


class CheckResultSetEmptyVerticaOperator(VerticaOperator):
    """abcs
    """

    def __init__(self, reverse=False, *args, **kwargs):
        self.reverse = reverse
        super().__init__(*args, **kwargs)
    
    def execute(self, context=None):
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        self.log.info(f'Executing SQL: {self.sql}')
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cur:
                res = cur.execute(self.sql)
                count = 0
                for _ in res.iterate():
                    count += 1
                    if not self.reverse and count > 0:
                        raise AirflowException('Result not empty')
                if self.reverse and count == 0:
                    raise AirflowException('Result is empty')
                self.log.info('Success!')


class CheckTableEmptyVerticaOperator(CheckResultSetEmptyVerticaOperator):
    """asdfsad
    """

    def __init__(self, target, date_column=None, truncate_date=False, *args, **kwargs):
        super().__init__(sql=COUNT,
                         params=dict(
                             target=target,
                             date_column=date_column,
                             truncate_date=truncate_date
                         ),
                         *args, **kwargs)


class AnalyzeConstraintsVerticaOperator(CheckResultSetEmptyVerticaOperator):
    """asdfasd
    """

    def __init__(self, target='', *args, **kwargs):
        super().__init__(sql=ANALYZE_CONSTRAINTS, params=dict(target=target), *args, **kwargs)


class CheckUniqueVerticaOperator(CheckResultSetEmptyVerticaOperator):
    """asdfsadf
    """

    def __init__(self, target, key='id', *args, **kwargs):
        super().__init__(sql=NON_UNIQUE_KEYS, params=dict(target=target, key=key), *args, **kwargs)


class CheckNoCommonKeysVerticaOperator(CheckResultSetEmptyVerticaOperator):
    """sadfsadf
    """

    def __init__(self, target, check_in, key='id', date_column=None, truncate_date=False, *args, **kwargs):
        super().__init__(sql=KEYS,
                         params=dict(
                             target=target,
                             check_in=check_in,
                             key=key,
                             date_column=date_column,
                             truncate_date=truncate_date,
                             include=False
                         ),
                         *args, **kwargs)


class CheckAllKeysVerticaOperator(CheckResultSetEmptyVerticaOperator):
    """asdfs nblkn
    """

    def __init__(self, target, check_in, key='id', date_column=None, truncate_date=False, *args, **kwargs):
        super().__init__(sql=KEYS,
                         params=dict(
                             target=target,
                             check_in=check_in,
                             key=key,
                             date_column=date_column,
                             truncate_date=truncate_date,
                             include=True
                         ),
                         *args, **kwargs)
