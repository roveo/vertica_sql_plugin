from airflow.contrib.operators.vertica_operator import VerticaOperator
from vertica_sql_plugin.sql import SWAP


class SwapVerticaOperator(VerticaOperator):
    """Swaps table_a with table_b, works only inside the same schema.

    Args:
        schema: Schema name.
        table_a: Table to swap.
        table_b: Table to swap ``table_a`` with.
    """

    def __init__(self, schema, table_a, table_b, view=False, prefix='__airflow_swap__', *args, **kwargs):
        """
        Args:
            schema:
            table_a:
            table_b:
            view:
            prefix:
            *args:
            **kwargs:
        """
        params = dict(schema=schema, table_a=table_a, table_b=table_b, view=view, prefix=prefix)
        super().__init__(sql=SWAP, params=params, *args, **kwargs)
