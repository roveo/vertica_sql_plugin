COPY = """
COPY {{ params.target }}
FROM STDIN {{ params.compression if params.compression else '' }}
DELIMITER '{{ params.delimiter }}'
SKIP {{ params.skip }}
{{ 'DIRECT' if params.direct else '' }}
{{ 'TRAILING NULLCOLS' if params.trailing_nullcols else '' }}
{{ 'ENFORCELENGTH' if params.enforcelength else '' }}
{{ 'ABORT ON ERROR' if params.abort_on_error else '' }}
{{ 'parser' if params.parser else '' }} {{ params.parser if params.parser else '' }}
;
"""
