====================================
Stored relations and transactions
====================================

In Cozo, data are stored in *stored relations* on disk.

---------------------------
Stored relations
---------------------------

To query stored relations,
use the ``*relation[...]`` or ``*relation{...}`` atoms in inline or fixed rules,
as explained in the last chapter.
To manipulate stored relations, use one of the following query options:

.. module:: QueryOp
    :noindex:

.. function:: :create <NAME> <SPEC>

    Create a stored relation with the given name and spec.
    No stored relation with the same name can exist beforehand.
    If a query is specified, data from the resulting relation is put into the newly created stored relation.
    This is the only stored relation-related query option in which a query may be omitted.

.. function:: :replace <NAME> <SPEC>

    Similar to ``:create``, except that if the named stored relation exists beforehand,
    it is completely replaced. The schema of the replaced relation need not match the new one.
    You cannot omit the query for ``:replace``.
    If there are any triggers associated, they will be preserved. Note that this may lead to errors if ``:replace``
    leads to schema change.

.. function:: :put <NAME> <SPEC>

    Put rows from the resulting relation into the named stored relation.
    If keys from the data exist beforehand, the corresponding rows are replaced with new ones.

.. function:: :ensure <NAME> <SPEC>

    Ensure that rows specified by the output relation and spec exist in the database,
    and that no other process has written to these rows when the enclosing transaction commits.
    Useful for ensuring read-write consistency.

.. function:: :rm <NAME> <SPEC>

    Remove rows from the named stored relation. Only keys should be specified in ``<SPEC>``.
    Removing a non-existent key is not an error and does nothing.

.. function:: :ensure_not <NAME> <SPEC>

    Ensure that rows specified by the output relation and spec do not exist in the database
    and that no other process has written to these rows when the enclosing transaction commits.
    Useful for ensuring read-write consistency.

You can rename and remove stored relations with the system ops ``::relation rename`` and ``::relation remove``,
described in the system op chapter.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Create and replace
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The format of ``<SPEC>`` is identical for all four ops, but the semantics is a bit different.
We first describe the format and semantics for ``:create`` and ``:replace``.

A spec, or a specification for columns, is enclosed in curly braces ``{}`` and separated by commas::

    ?[address, company_name, department_name, head_count] <- $input_data

    :create dept_info {
        company_name: String,
        department_name: String,
        =>
        head_count: Int,
        address: String,
    }

Columns before the symbol ``=>`` form the *keys* (actually a composite key) for the stored relation,
and those after it form the *values*.
If all columns are keys, the symbol ``=>`` may be omitted.
The order of columns matters.
Rows are stored in lexicographically sorted order in trees according to their keys.

In the above example, we explicitly specified the types for all columns.
In case of type mismatch,
the system will first try to coerce the values given, and if that fails, the query is aborted with an error.
You can omit types for columns, in which case their types default to ``Any?``,
i.e. all values are acceptable.
For example, the above query with all types omitted is::

    ?[address, company_name, department_name, head_count] <- $input_data

    :create dept_info { company_name, department_name => head_count, address }

In the example, the bindings for the output match the columns exactly (though not in the same order).
You can also explicitly specify the correspondence::

    ?[a, b, count(c)] <- $input_data

    :create dept_info {
        company_name = a,
        department_name = b,
        =>
        head_count = count(c),
        address: String = b
    }

You *must* use explicit correspondence if the entry head contains aggregation,
since names such as ``count(c)`` are not valid column names.
The ``address`` field above shows how to specify both a type and a correspondence.

Instead of specifying bindings, you can specify an expression that generates default values by using ``default``::

    ?[a, b] <- $input_data

    :create dept_info {
        company_name = a,
        department_name = b,
        =>
        head_count default 0,
        address default ''
    }

The expression is evaluated anew for each row, so if you specified a UUID-generating functions,
you will get a different UUID for each row.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Put, remove, ensure and ensure-not
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For ``:put``, ``:remove``, ``:ensure`` and ``:ensure_not``,
you do not need to specify all existing columns in the spec if the omitted columns have a default generator,
or if the type of the column is nullable, in which case the value defaults to ``null``.
For these operations, specifying default values does not have any effect and will not replace existing ones.

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

The ``:assert (some|none)``, ``:ensure`` and ``:ensure_not`` query options allow you to express complicated constraints
that must be satisfied for your transaction to commit.

This example uses three queries to put and remove rows atomically
(either all succeed or all fail), and ensure that at the end of the transaction
an untouched row exists::

    {
        ?[a, b] <- [[1, 'one'], [3, 'three']]
        :put rel {a => b}
    }
    {
        ?[a] <- [[2]]
        :rm rel {a}
    }
    {
        ?[a, b] <- [[4, 'four']]
        :ensure rel {a => b}
    }

When a transaction starts, a snapshot is used,
so that only already committed data,
or data written within the same transaction, are visible to queries.
At the end of the transaction, changes are only committed if there are no conflicts
and no errors are raised.
If any mutation activate triggers, those triggers execute in the same transaction.

------------------------------------------------------
Triggers and indices
------------------------------------------------------

Cozo does not have traditional indices on stored relations.
Instead, you define regular stored relations that are used as indices.
At query time, you explicitly query the index instead of the original stored relation.

You synchronize your indices and the original by ensuring that any mutations you do on the database
write the correct data to the "canonical" relation and its indices in the same transaction.
As doing this by hand for every mutation leads to lots of repetitions
and is error-prone,
Cozo supports *triggers* to do it automatically for you.

You attach triggers to a stored relation by running the system op ``::set_triggers``::

    ::set_triggers <REL_NAME>

    on put { <QUERY> }
    on rm { <QUERY> }
    on replace { <QUERY> }
    on put { <QUERY> } # you can specify as many triggers as you need

``<QUERY>`` can be any valid query.

The ``on put`` triggers will run when new data is inserted or upserted,
which can be activated by ``:put``, ``:create`` and ``:replace`` query options.
The implicitly defined rules ``_new[]`` and ``_old[]`` can be used in the triggers, and
contain the added rows and the replaced rows respectively.

The ``on rm`` triggers will run when data is deleted, which can be activated by a ``:rm`` query option.
The implicitly defined rules ``_new[]`` and ``_old[]`` can be used in the triggers,
and contain the keys of the rows for deleted rows (even if no row with the key actually exist) and the rows
actually deleted (with both keys and non-keys).

The ``on replace`` triggers will be activated by a ``:replace`` query option.
They are run before any ``on put`` triggers.

All triggers for a relation must be specified together, in the same ``::set_triggers`` system op.
If used again, all the triggers associated with the stored relation are replaced.
To remove all triggers from a stored relation, use ``::set_triggers <REL_NAME>`` followed by nothing.

As an example of using triggers to maintain an index, suppose we have the following relation::

    :create rel {a => b}

We often want to query ``*rel[a, b]`` with ``b`` bound but ``a`` unbound. This will cause a full scan,
which can be expensive. So we need an index::

    :create rel.rev {b, a}

In the general case, we cannot assume a functional dependency ``b => a``, so in the index both fields appear as keys.

To manage the index automatically::

    ::relation set_triggers rel

    on put {
        ?[a, b] := _new[a, b]

        :put rel.rev{ b, a }
    }
    on rm {
        ?[a, b] := _old[a, b]

        :rm rel.rev{ b, a }
    }

With the index set up, you can use ``*rel.rev{..}`` in place of ``*rel{..}`` in your queries.

Indices in Cozo are manual, but extremely flexible, since you need not conform to any predetermined patterns
in your use of ``_old[]`` and ``_new[]``.
For simple queries, the need to explicitly elect to use an index can seem cumbersome,
but for complex ones, the deterministic evaluation entailed can be a huge blessing.

Triggers can be creatively used for other purposes as well.

.. WARNING::

    Loops in your triggers can cause non-termination.
    A loop occurs when a relation has triggers which affect other relations,
    which in turn have other triggers that ultimately affect the starting relation.