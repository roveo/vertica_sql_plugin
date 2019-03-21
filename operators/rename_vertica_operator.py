from airflow.contrib.operators.vertica_operator import VerticaOperator
from ..sql import RENAME



class RenameVerticaOperator(VerticaOperator):

    def __init__(self, schema, name, new_name, view=False, *args, **kwargs):
        params = dict(schema=schema, name=name, new_name=new_name, view=view)
        super().__init__(sql=RENAME, params=params, *args, **kwargs)
