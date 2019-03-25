from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook
from airflow.models import SkipMixin
from airflow.exceptions import AirflowException


class ShortCircuitVerticaOperator(VerticaOperator, SkipMixin):

    def __init__(self, test, *args, **kwargs):
        self.test = test
        super(ShortCircuitVerticaOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.log.info(f'Running SQL query {self.sql}')
        hook = VerticaHook(vertica_conn_id=self.vertica_conn_id)
        result_set = hook.get_records(self.sql)
        condition = self.test(result_set)

        if condition:
            self.log.info('Proceeding with downstream tasks...')
            return

        self.log.info('Skipping downstream tasks...')

        downstream_tasks = context['task'].get_flat_relatives(upstream=False)
        self.log.debug("Downstream task_ids %s", downstream_tasks)

        if downstream_tasks:
            self.skip(context['dag_run'], context['ti'].execution_date, downstream_tasks)

        self.log.info("Done.")
