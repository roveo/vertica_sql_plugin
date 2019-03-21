from airflow.contrib.operators.vertica_operator import VerticaOperator
from ..sql import SWAP


class SwapVerticaOperator(VerticaOperator):

    def __init__(self, schema, table_a, table_b, view=False, *args, **kwargs):
        params = dict(schema=schema, table_a=table_a, table_b=table_b, view=view)
        super().__init__(sql=SWAP, params=params, *args, **kwargs)
