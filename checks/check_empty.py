from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.exceptions import AirflowException
from vertica_sql_plugin.sql import COUNT


class CheckEmptyVerticaOperator(VerticaOperator):

    def __init__(self, target, date_column=None, reverse=False, *args, **kwargs):
        self.reverse = reverse
        params = dict(target=target, date_column=date_column)
        super().__init__(sql=COUNT, params=params, *args, **kwargs)
    
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
