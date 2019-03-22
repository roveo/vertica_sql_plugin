# Vertica SQL Airflow Plugin

This plugin provides operators for common SQL operations in HP Vertica. You can always write SQL yourself, but I figured it's nice to have standard blocks to build with.

## Actions

This set of operators covers SQL standard statements and some common operations on the database.

### `InsertVerticaOperator`

This is the workhorse of data processing. When instantiated, it tries to list the target table's columns and expects to see them in the `source`.

`target` Table to insert the data into.

`source` Table or view to select from.

`date_column` _(optional)_ If provided, the selected data will be filtered on this column, starting with `execution_date` and ending at `next_execution_date` (not included).

`truncate_date` Set this to `True` if you want execution_date to be truncated to date. This is useful if you run your daily incremental update at, say, 06:00 in the morning, but want to process the data from 00:00 to 23:59 yesterday.

`column_mapping` Set this to a `dict` mapping from target to source columns. If source and target column names match, keep in `None`.

So,

```python
{
    'ts': 'TransactionTimestamp',
    'amount': 'TransactionAmount'
}
```

will produce this SQL query:

```sql
INSERT INTO target_table (ts, amount)
SELECT TransactionTimestamp, TransactionAmount
FROM source_table;
```

`force_introspection` If you'd like to partially override the introspected columns, set this to `True`. The column_mapping will be then merged into the mapping resulting from introspection.

`direct` Will set a Vertica `/* +direct */` hint for the INSERT statement.

`vertica_conn_id` argument from the `VerticaOperator` parent class.

__Don't forget your `task_id`!__

### `DeleteVerticaOperator`

`TruncateVerticaOperator`

`CreateTableLikeVerticaOperator`

`RenameVerticaOperator`

`SwapVerticaOperator`

### Checks
