from airflow.plugins_manager import AirflowPlugin
from airflow.contrib.operators.vertica_operator import VerticaOperator
from airflow.contrib.hooks.vertica_hook import VerticaHook


SELECT_INSERT = """
INSERT {{ '/* +direct */' if params.direct else '' }}INTO {{ params.target }}
(
    {{ params.target_columns }}
)
SELECT {{ params.source_columns }}
FROM {{ params.source }}{% if params.date_column  %}
WHERE {{ params.date_column }} >= '{{ execution_date.strftime('%Y-%m%-%d %H:%M:%S') }}'::timestamp
    and {{ params.date_column }} < '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S') }}'::timestamp
{% endif %};
"""

TRUNCATE = """
TRUNCATE TABLE {{ params.target }};
"""

DELETE = """
BEGIN;
DELETE {{ '/* +direct */' if params.direct else '' }}
FROM {{ params.target }}
{% if params.date_column  %}{% if params.date_column  %}
WHERE {{ params.date_column }} >= '{{ execution_date.strftime('%Y-%m%-%d %H:%M:%S') }}'::timestamp
    and {{ params.date_column }} < '{{ next_execution_date.strftime('%Y-%m-%d %H:%M:%S') }}'::timestamp
{% endif %};
COMMIT;
"""

CREATE_TABLE_LIKE = """
BEGIN;
CREATE TABLE {{ params.target }}
    LIKE {{ params.source }}{{ 'INCLUDING PROJECTIONS' if params.projections else '' }};
COMMIT;
"""

RENAME_TABLE = """
BEGIN;
ALTER {{ 'VIEW' if params.view else 'TABLE' }} {{ params.schema }}.{{ params.name }}
    RENAME TO {{ params.new_name }};
COMMIT;
"""

CHANGE_SCHEMA = """
BEGIN;
ALTER {{ 'VIEW' if params.view else 'TABLE' }} {{ params.target }}
    SET SCHEMA {{ params.new_schema }};
COMMIT;
"""

GET_TABLE_COLUMNS = """
SELECT column_name
FROM columns
WHERE table_schema || '.' || table_name = :table
ORDER BY ordinal_position;
"""

SWAP = """
BEGIN;
ALTER {{ 'VIEW' if params.view else 'TABLE' }} {{ params.schema }}.{{ params.table_a }}
    RENAME TO {{ params.prefix }}{{ params.table_b }};
ALTER {{ 'VIEW' if params.view else 'TABLE' }} {{ params.schema }}.{{ params.table_b }}
    RENAME TO {{ params.table_a }};
ALTER {{ 'VIEW' if params.view else 'TABLE' }} {{ params.schema }}.{{ params.prefix }}{{ params.table_b }}
    RENAME TO {{ params.table_b }};
COMMIT;
"""


class TruncateVerticaOperator(VerticaOperator):

    def __init__(self, target, *args, **kwargs):
        params = dict(target=target)
        super().__init__(sql=TRUNCATE, params=params, *args, **kwargs)


class DeleteVerticaOperator(VerticaOperator):

    def __init__(self, target, date_column=None, direct=False, *args, **kwargs):
        params = dict(target=target, date_column=date_column, direct=direct)
        super().__init__(sql=DELETE, params=params, *args, **kwargs)


class SelectInsertVerticaOperator(VerticaOperator):

    def __init__(self, source, target, date_column=None, direct=False, column_mapping=None, force_introspection=False,
                 exclude=None,
                 *args, **kwargs):
        if column_mapping is None or force_introspection:
            exclude = exclude or []
            vertica_conn_id = kwargs.get('vertica_conn_id', 'vertica_default')
            hook = VerticaHook(vertica_conn_id=vertica_conn_id)
            columns = {c: c for (c,) in hook.get_records(GET_TABLE_COLUMNS, dict(table=target)) if c not in exclude}
        column_mapping = column_mapping or {}
        columns.update(column_mapping)
        source_columns, target_columns = list(zip(*columns.items()))
        params = dict(source=source, target=target, date_column=date_column, direct=direct,
                      source_columns=', '.join(source_columns), target_columns=', '.join(target_columns))
        super().__init__(sql=SELECT_INSERT, params=params, *args, **kwargs)


class RenameVerticaOperator(VerticaOperator):

    def __init__(self, schema, name, new_name, view=False, *args, **kwargs):
        params = dict(schema=schema, name=name, new_name=new_name, view=view)
        super().__init__(sql=RENAME_TABLE, params=params, *args, **kwargs)


class SwapVerticaOperator(VerticaOperator):

    def __init__(self, schema, table_a, table_b, view=False, *args, **kwargs):
        params = dict(schema=schema, table_a=table_a, table_b=table_b, view=view)
        super().__init__(sql=SWAP, params=params, *args, **kwargs)


class VerticaSqlPlugin(AirflowPlugin):

    name = 'vertica_sql'