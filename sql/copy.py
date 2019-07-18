COPY = """
BEGIN;
    {{ 'TRUNCATE TABLE' if params.truncate else '' }} {{ params.target if params.truncate else '' }}; 
    COPY {{ params.target }}
    FROM STDIN {{ params.compression if params.compression else '' }}
    {{ 'parser' if params.parser else '' }} {{ params.parser if params.parser else '' }}
    DELIMITER '{{ params.delimiter }}'
    SKIP {{ params.skip }}
    {{ 'DIRECT' if params.direct else '' }}
    {{ 'TRAILING NULLCOLS' if params.trailing_nullcols else '' }}
    {{ 'ENFORCELENGTH' if params.enforcelength else '' }}
    {{ 'ABORT ON ERROR' if params.abort_on_error else '' }}
    {{ 'REJECTED DATA AS TABLE' if params.rejected_data_as_table else '' }}
    {{ params.rejected_data_as_table if params.rejected_data_as_table else '' }}
    {{ 'ENCLOSED BY' if params.enclosed_by else '' }} '{{ params.enclosed_by if params.enclosed_by else '' }}';
COMMIT;
"""
