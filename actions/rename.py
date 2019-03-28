from airflow.contrib.operators.vertica_operator import VerticaOperator
from vertica_sql_plugin.sql import RENAME


class RenameVerticaOperator(VerticaOperator):
    """Rename table or view in database. Works only inside the same schema.

    Args:
        task_id: Task ID for Airflow Operator.
        vertica_conn_id: Connection ID for Vertica.

        schema: Objects' schema name.
        name: Object's name.
        new_name: The name to be set.
        view: Rename a view instead of a table.

    """

    def __init__(self, schema, name, new_name, view=False, *args, **kwargs):
        params = dict(schema=schema, name=name, new_name=new_name, view=view)
        super().__init__(sql=RENAME, params=params, *args, **kwargs)
