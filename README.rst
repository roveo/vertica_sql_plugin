
Vertica SQL Plugin for Apache Airflow
*************************************

Operators for common SQL actions.


Installation
============

Clone this into ``$AIRFLOW_HOME/plugins``.


Usage
=====

Just import as a usual Airflow plugin. To get proper IDE behaviour,
try this:

::

   try:
       import vertica_sql_plugin as vsql  # IDE will pick this up
   except ImportError:
       import airflow.vertica_sql.operators as vsql  # this will be used by airflow


Operators
=========


Basic SQL Actions
-----------------

**class vertica_sql_plugin.TruncateVerticaOperator(target, *args,
**kwargs)**

   Truncates a table.

   :Parameters:
      * **target** – Table to truncate.

      * **vertica_conn_id** – Airflow connection ID.

   Note: For large tables, better use ``DeleteVerticaOperator`` with
      ``direct=True``.

**class vertica_sql_plugin.DeleteVerticaOperator(target,
date_column=None, truncate_date=False, direct=False, *args,
**kwargs)**

**class vertica_sql_plugin.InsertVerticaOperator(source, target,
date_column=None, truncate_date=False, direct=False,
column_mapping=None, force_introspection=False, exclude=None, *args,
**kwargs)**

   Inserts data from source table or view into target table.

   :Parameters:
      * **source** (*str*) – Source table or view.

      * **target** (*str*) – Target table.

      * **date_column** (*str*) – Defaults to ``None``. If provided,
         the source will be additionally filtered on this column to be
         ``>= execution_date and < next_execution_date``.

      * **truncate_date** (*bool*) –

         Defaults to ``None``.

         If true, ``execution_date`` will be truncated to date. This
         is useful when your ``start_date`` is in the morning but the
         processed data should be between 00:00 and 00:00 next day.

      * **direct** (*bool*) –

         Defaults to ``False``.

         Will add a ``/* +direct */`` Vertica directive to the INSERT
         statement.

      * **column_mapping** (*dict*) –

         Defaults to ``None``.

         A mapping from target to source columns. If provided, the
         operator won’t run database introspection to infer these
         automatically, so only mapped columns will be inserted.

      * **force_introspection** (*bool*) –

         Defaults to ``False``.

         If provided together with ``column_mapping``, will force the
         database introspection anyway and merge the user-provided
         mapping into the result of introspection. Use this when most
         column names in source and target are the same, but some
         aren’t.

      * **exclude** (*list*) –

         Defaults to ``None``.

         List of columns to not select from the source. Use this for
         columns created with a ``default`` option.
