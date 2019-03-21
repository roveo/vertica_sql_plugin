from airflow.plugins_manager import AirflowPlugin


class VerticaSqlPlugin(AirflowPlugin):

    name = 'vertica_sql'
    operators = [
        SelectInsertVerticaOperator,
        TruncateVerticaOperator,
        DeleteVerticaOperator,
        RenameVerticaOperator,
        SwapVerticaOperator
    ]
