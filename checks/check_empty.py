from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.exceptions import AirflowException
from vertica_sql_plugin.sql import (
    COUNT,
    ANALYZE_CONSTRAINTS,
    NON_UNIQUE_KEYS,
    COMMON_KEYS
)


class CheckResultSetEmptyVerticaOperator(VerticaOperator):

    def __init__(self, reverse=False, *args, **kwargs):
        self.reverse = reverse
        super().__init__(*args, **kwargs)
    
    def execute(self, context=None):
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        self.log.info(f'Executing SQL: {self.sql}')
        (count,) = hook.get_first(self.sql)
        self.log.info(f'The result is {count} rows')
        if count > 0 and not self.reverse:
            raise AirflowException('Result not empty')
        elif count == 0 and self.reverse:
            raise AirflowException('Result is empty')
        self.log.info('Success!')


class CheckTableEmptyVerticaOperator(CheckResultSetEmptyVerticaOperator):

    def __init__(self, target, date_column=None, *args, **kwargs):
        super().__init__(sql=COUNT, params=dict(target=target, date_column=date_column), *args, **kwargs)


class AnalyzeConstraintsVerticaOperator(CheckResultSetEmptyVerticaOperator):

    def __init__(self, target='', *args, **kwargs):
        super().__init__(sql=ANALYZE_CONSTRAINTS, params=dict(target=target), *args, **kwargs)


class CheckUniqueVerticaOperator(CheckResultSetEmptyVerticaOperator):

    def __init__(self, target, key, *args, **kwargs):
        super().__init__(sql=NON_UNIQUE_KEYS, params=dict(target=target, key=key), *args, **kwargs)


class CheckNoCommonKeysVerticaOperator(CheckResultSetEmptyVerticaOperator):

    def __init__(self, table_a, table_b, key='id', *args, **kwargs):
        super().__init__(sql=COMMON_KEYS, params=dict(table_a=table_a, table_b=table_b, key=key), *args, **kwargs)
