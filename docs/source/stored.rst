====================================
Stored relations and transactions
====================================

Persistent databases store data on disk. As Cozo is a relational database,
data are stored in *stored relations* on disk, which is analogous to tables in SQL databases.

---------------------------
Using stored relations
---------------------------



------------------------------------------------------
Chaining queries into a single transaction
------------------------------------------------------


The first thing to know before we get into the topic is that you can execute multiple queries in one go,
by wrapping each query in curly braces ``{}``. Each query can have its own independent query options.
Execution proceeds for each query serially, and aborts at the first error encountered.
The returned relation is that of the last query.

Multiple queries passed in one go are executed in a single transaction. Within the transaction,
execution of queries adheres to multi-version concurrency control: only data that are already committed,
or written within the same transaction, are read,
and at the end of the transaction, any changes to stored relations are only committed if there are no conflicts
and no errors are raised.


------------------------------------------------------
Triggers and ad-hoc indices
------------------------------------------------------