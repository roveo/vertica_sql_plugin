from airflow.plugins_manager import AirflowPlugin
from vertica_sql_plugin.operators import *


class VerticaSqlPlugin(AirflowPlugin):

    name = 'vertica_sql'
    operators = [
        InsertVerticaOperator,
        TruncateVerticaOperator,
        DeleteVerticaOperator,
        RenameVerticaOperator,
        SwapVerticaOperator,
        CreateTableLikeVerticaOperator
    ]
