# Simplified SQL Query Engine
An SQL engine which can execute a query written in a simplified form of Structured Query Language (SQL) against a simple database.

## Simplified SQL
This SQL query engine handles a simplified version of SQL, supporting the following features:

* A **SELECT** clause, with any number of columns.
* A **FROM** clause, which identifies the primary table to select records from.
* Any number of optional **JOIN** clauses, treated as **INNER JOINs**.
*An optional WHERE clause with only one condition.

Unsupported Features
The engine does not support:

* Aliases (AS or [bracketed names]).
* CASTing.
* ORDER BY, GROUP BY, COUNT, or EXISTS.
* IN or LIKE, or any other operator other than simple equality, inequality, and greater than/less than.
* AND or OR; only a single WHERE condition is allowed.

## SQL grammer
The SQL queries must adhere to the following EBNF grammar:

```text
query         =  select, ws, from, [ ws, join ], [ ws, where ] ;
select        =  "SELECT ", column-id, [ { ", ", column-id } ] ;
from          =  "FROM ", table-name, [ { ws, join } ] ;
join          =  "JOIN ", table-name, " on ", value-test ;
where         =  "WHERE ", value-test ;
value-test    =  value, comparison, value;
column-id     =  table-name, ".", column-name ;
table-name    = ? a valid SQL table name ? ;
column-name   = ? a valid SQL column name ? ;
value         =  column-id | const
comparison    =  " = " | " > " | " < " | " <= " | " >= " | " <> " ;
const         =  ? a number ? | ? a SQL single-quoted string ? ;
ws            = " " | "\n" | ws, ws ;
```

Note that SQL is not case sensitive; `SELECT` and `select` are equivalent.

### Valid Table and Column Names
A "valid SQL table/column name" is a name containing only letters, numbers, and the '_' character, with no spaces (`/[a-zA-Z0-9_]+/`).
