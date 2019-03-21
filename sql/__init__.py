INSERT = """
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
{% if params.date_column  %}
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


RENAME = """
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
