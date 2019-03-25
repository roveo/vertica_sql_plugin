from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook


class VerticaSensor(BaseSensorOperator):

    def __init__(self, vertica_conn_id='vertica_default',
                 checker=lambda: True,
                 sql='select 1',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hook = VerticaHook(vertica_conn_id=vertica_conn_id)
        self.checker = checker
        self.sql = sql

    def poke(self, context):
        self.log.info(f'Poking: {self.sql}')
        try:
            response = self.hook.get_records(self.sql)
            return self.checker(response)
        except Exception as e:
            return False
            raise e
        return True
