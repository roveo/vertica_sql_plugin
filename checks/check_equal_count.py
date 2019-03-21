from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.exceptions import AirflowException
from vertica_sql_plugin.sql import EQUAL_COUNT


class CheckEqualCountVerticaOperator(VerticaOperator):

    def __init__(self, table_a, table_b, date_column=None, reverse=False, *args, **kwargs):
        self.reverse = reverse
        params = dict(table_a=table_a, table_b=table_b, date_column=date_column)
        super().__init__(sql=EQUAL_COUNT, params=params *args, **kwargs)
    
    def execute(self, context=None):
        self.log.info(f'Executing SQL: %s', self.sql)
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        records = hook.get_records(self.sql)
        self.log.info(f'Returned rows: {len(records)}')
        if len(records) == 2 and not self.reverse:
            raise AirflowException('Row counts are not equal')
        elif len(records) == 1 and self.reverse:
            raise AirflowException('Row counts are equal')
        self.log.info("Success.")
