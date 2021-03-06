from airflow.plugins_manager import AirflowPlugin
from vertica_sql_plugin.actions import *
from vertica_sql_plugin.checks import *
from vertica_sql_plugin.operators import *


class VerticaSqlPlugin(AirflowPlugin):

    name = 'vertica_sql'
    operators = [
        # actions
        InsertVerticaOperator,
        TruncateVerticaOperator,
        DeleteVerticaOperator,
        RenameVerticaOperator,
        SwapVerticaOperator,
        CreateTableLikeVerticaOperator,
        CopyFromStdinVerticaOperator,
        DeleteInsertVerticaOperator,

        # checks
        CheckResultSetEmptyVerticaOperator,
        CheckTableEmptyVerticaOperator,
        AnalyzeConstraintsVerticaOperator, 
        CheckUniqueVerticaOperator,
        CheckNoCommonKeysVerticaOperator,
        CheckAllKeysVerticaOperator,

        # operators
        VerticaSensor,
        ShortCircuitVerticaOperator
    ]
