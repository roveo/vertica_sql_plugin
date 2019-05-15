from vertica_sql_plugin.sql import DELETE_INSERT
from vertica_sql_plugin.actions.insert import InsertVerticaOperator


class DeleteInsertVerticaOperator(InsertVerticaOperator):

    def __init__(self, *args, **kwargs):
        super().__init__(sql=DELETE_INSERT, *args, **kwargs)
