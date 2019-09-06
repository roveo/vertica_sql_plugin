Vertica SQL Plugin for Apache Airflow
=====================================

Operators for common SQL actions.

Installation
------------

Clone this into ``$AIRFLOW_HOME/plugins``.

Usage
-----

Just import as a usual Airflow plugin. To get proper IDE behaviour, try this:

.. code-block:: python

    try:
        import vertica_sql_plugin as vsql  # IDE will pick this up
    except ImportError:
        import airflow.vertica_sql.operators as vsql  # this will be used by airflow

Operators
---------

Basic SQL Actions
~~~~~~~~~~~~~~~~~

.. autoclass:: vertica_sql_plugin.TruncateVerticaOperator
.. autoclass:: vertica_sql_plugin.DeleteVerticaOperator
.. autoclass:: vertica_sql_plugin.CreateTableLikeVerticaOperator
.. autoclass:: vertica_sql_plugin.SwapVerticaOperator
.. autoclass:: vertica_sql_plugin.InsertVerticaOperator
.. autoclass:: vertica_sql_plugin.CopyFromStdinVerticaOperator
