
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
      * **task_id** – Task ID for Airflow Operator.

      * **vertica_conn_id** – Connection ID for Vertica.

      * **target** – Table to truncate.

   Note: For large tables, better use ``DeleteVerticaOperator`` with
      ``direct=True``.

**class vertica_sql_plugin.DeleteVerticaOperator(target,
date_column=None, truncate_date=False, direct=False, *args,
**kwargs)**

   Deletes rows from target table.

   :Parameters:
      * **task_id** – Task ID for Airflow Operator.

      * **vertica_conn_id** – Connection ID for Vertica.

      * **target** – Table to delete the rows from.

      * **date_column** –

         Defaults to ``None``.

         If provided, the ``DELETE`` statement will have a ``WHERE``
         clause that’s ``>= execution_date AND <
         next_execution_date``.

**class vertica_sql_plugin.CreateTableLikeVerticaOperator(target,
source, projections=False, *args, **kwargs)**

   Creates a new empty table that’s a copy of an existing one.

   :Parameters:
      * **task_id** – Task ID for Airflow Operator.

      * **vertica_conn_id** – Connection ID for Vertica.

      * **target** – Which table to create.

      * **source** – Which table to use as source.

      * **projections** (*bool*) –

         Defaults to ``False``.

         Whether to include Vertica ``INCLUDING PROJECTIONS``
         directive.

**class vertica_sql_plugin.SwapVerticaOperator(schema, table_a,
table_b, view=False, prefix='__airflow_swap__', *args, **kwargs)**

   Swaps table_a with table_b, works only inside the same schema.

   :Parameters:
      * **task_id** – Task ID for Airflow Operator.

      * **vertica_conn_id** – Connection ID for Vertica.

      * **schema** – Schema name.

      * **table_a** – Table to swap.

      * **table_b** – Table to swap table_a with.

      * **view** – Swap views and not tables.

      * **prefix** –

         Defaults to ``'__airflow_swap__'``.

         Prefix to use when creating intermediary table.

**class vertica_sql_plugin.InsertVerticaOperator(source, target,
date_column=None, truncate_date=False, direct=False,
column_mapping=None, force_introspection=False, exclude=None, *args,
**kwargs)**

   Inserts data from source table or view into target table.

   If ``column_mapping`` is not provided by the user, it will run an
   inspection on the target table to list it’s columns. Column names
   are expected to be the same in the source. Note that the inspection
   is run when the DAG is created, not when it’s run, so be aware of
   how much strain it puts on the database.

   :Parameters:
      * **task_id** – Task ID for Airflow Operator.

      * **vertica_conn_id** – Connection ID for Vertica.

      * **source** – Source table or view.

      * **target** – Target table.

      * **date_column** –

         Defaults to ``None``.

         If provided, the source will be additionally filtered on this
         column to be ``>= execution_date and < next_execution_date``.

      * **truncate_date** (*bool*) –

         Defaults to ``None``.

         If True, ``execution_date`` will be truncated to date. This
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
