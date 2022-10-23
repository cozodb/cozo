====================================
Stored relations and transactions
====================================

Persistent databases store data on disk. As Cozo is a relational database,
data are stored in *stored relations* on disk, which is analogous to tables in SQL databases.

---------------------------
Stored relations
---------------------------

We already know how to query stored relations: 
use the ``:relation[...]`` or ``:relation{...}`` atoms in inline or fixed rules.
To manipulate stored relations, use one of the following query options:

.. module:: QueryOp
    :noindex:

.. function:: :create <NAME> <SPEC>

    Creates a stored relation with the given name and the given spec. 
    The named stored relation must not exist before.
    If a query is specified, data from the resulting relation is put into the created stored relation.
    This is the only stored relation-related query option in which a query may be omitted.

.. function:: :replace <NAME> <SPEC>

    This is similar to ``:create``, except that if the named stored relation exists beforehand, 
    it is completely replaced. The schema of the replaced relation need not match the new one.
    You cannot omit the query for ``:replace``.

.. function:: :put <NAME> <SPEC>

    Put data from the resulting relation into the named stored relation.
    If keys from the data exist beforehand, the rows are simply replaced with new ones.

.. function:: :ensure <NAME> <SPEC>

    Ensures that rows specified by the output relation and spec already exist in the database
    and that no other process has written to these rows at commit since the transaction starts.
    Useful for ensuring read-write consistency.

.. function:: :rm <NAME> <SPEC>

    Remove data from the resulting relation from the named stored relation.
    Only keys are used.
    If a row from the resulting relation does not match any keys, nothing happens for that row,
    and no error is raised.

.. function:: :ensure_not <NAME> <SPEC>

    Ensures that rows specified by the output relation and spec do not exist in the database
    and that no other process has written to these rows at commit since the transaction starts.
    Useful for ensuring read-write consistency.

You can rename and remove stored relations with the system ops ``::relation rename`` and ``::relation remove``,
described in the system op chapter.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create and replace
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The format of ``<SPEC>`` is identical for all four ops, whereas the semantics is a bit different.

We first describe the format and semantics for ``:create`` and ``:replace``.
A spec is a specification for columns, enclosed in curly braces ``{}`` and separated by commas::

    ?[address, company_name, department_name, head_count] <- $input_data

    :create dept_info {
        company_name: String,
        department_name: String,
        =>
        head_count: Int,
        address: String,
    }

Columns before the symbol ``=>`` form the *keys* (actually, a composite key) for the stored relation,
and those after it form the *values*.
If all columns are keys, the symbol ``=>`` may be omitted altogether.
The order of columns matters in the specification,
especially for keys, as data is stored in lexicographically sorted order in trees,
which has implications for data access in queries.
Each key corresponds to a single value.

In the above example, we explicitly specified the types for all columns.
Type specification is described in its own chapter.
If the types of the rows do not match the specified types,
the system will first try to coerce the values, and if that fails, the query is aborted.
You can selectively omit types for columns, and columns with types omitted will have the type ``Any?``,
which is valid for any value.
As an example, if you do not care about type validation, the above query can be written as::

    ?[address, company_name, department_name, head_count] <- $input_data

    :create dept_info { company_name, department_name => head_count, address }

In the example, the bindings for the output match the columns exactly (though not in the same order).
You can also explicitly specify the correspondence::

    ?[a, b, count(c)] <- $input_data

    :create dept_info { company_name = a, department_name = b, => head_count = count(c), address = b }

You *must* use explicit correspondence if the entry head contains aggregation.

Instead of specifying bindings, you can specify an expression to generate values::

    ?[a, b] <- $input_data

    :create dept_info { company_name = a, department_name = b, => head_count default 0, address default '' }

The expression is evaluated once for each row, so for example if you specified one of the UUID-generating functions,
you will get a different UUID for each row.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Put, remove, ensure and ensure-not
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For ``:put``, ``:remove``, ``:ensure`` and ``:ensure_not``,
you do not need to specify all existing columns in the spec if the omitted columns have a default generator,
in which case the generator will be used to generate a value,
or the type of the column is nullable, in which case the value is ``null``.
The spec specified when the relation was created will be consulted to know how to store data correctly.
Specifying default values does not have any effect and will not replace existing ones.

For ``:put`` and ``:ensure``, the spec needs to contain enough bindings to generate all keys and values.
For ``:rm`` and ``:ensure_not``, it only needs to generate all keys.

------------------------------------------------------
Chaining queries
------------------------------------------------------

Each script you send to Cozo is executed in its own transaction.
To ensure consistency of multiple operations on data,
You can define multiple queries in a single script,
by wrapping each query in curly braces ``{}``.
Each query can have its independent query options.
Execution proceeds for each query serially, and aborts at the first error encountered.
The returned relation is that of the last query.
Within a transaction,
execution of queries adheres to multi-version concurrency control: only data that are already committed,
or written within the same transaction, are read,
and at the end of the transaction, any changes to stored relations are only committed if there are no conflicts
and no errors are raised.

The ``:assert``, ``:ensure`` and ``:ensure_not`` query options allow you to express complicated constraints
that must be satisfied for your transaction to commit.

------------------------------------------------------
Triggers
------------------------------------------------------

Cozo does not have traditional indices on stored relations.
You must define your indices as separate stored relations yourself,
for example by having a relation containing identical data but in different column order.
More complicated and exotic "indices" are also possible and used in practice.
At query time, you explicitly query the index instead of the original stored relation.

You synchronize your indices and the original by ensuring that any mutations you do on the database
write the correct data to the "canonical" relation and its indices in the same transaction.
As doing this by hand for every mutation in your business logic leads to lots of repetitions,
is error-prone and a maintenance nightmare,
Cozo also supports *triggers* to do it automatically for you.

You attach triggers to a stored relation by running the system op ``::relation set_triggers``::

    ::relation set_triggers relation_name

    on put { <QUERY> }
    on put { <QUERY> } # you can specify as many triggers as you need
    on rm { <QUERY> }
    on replace { <QUERY> }

You can have anything valid query for ``<QUERY>``.

The ``on put`` queries will run when any data is inserted into the relation,
which can be triggered by ``:put``, ``:create`` and ``:replace`` query options.
The implicitly defined rules ``_new[]`` and ``_old[]`` can be used in the queries, and
contain the added rows, and the replaced rows (if any).

The ``on rm`` queries will run when deletion is triggered by the ``:rm`` query option.
The implicitly defined rules ``_new[]`` and ``_old[]`` can be used in the queries,
the first rule contains the keys of the rows for deletion, and the second rule contains the rows
actually deleted, with both keys and non-keys.

The ``on replace`` queries will run when ``:replace`` query options are run.
They are run before any ``on put`` triggers are run for the same stored relation.

All triggers for a relation must be specified together, in the same system op.
In other words, ``::relation set_triggers`` simply replaces all the triggers associated with a stored relation.
To remove all triggers from a stored relation, simply pass no queries for the system op.

Besides indices, creative use of triggers abounds, but you must consider the maintenance burden they introduce.

.. WARNING::

    Do not introduce loops in your triggers.
    A loop occurs when a relation has triggers which affect other relations,
    which in turn have other triggers that ultimately affect the starting relation.