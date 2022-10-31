==============
Queries
==============

CozoScript, a `Datalog <https://en.wikipedia.org/wiki/Datalog>`_ dialect, is the query language of Cozo.

A CozoScript query consists of one or many named rules.
Each named rule represents a *relation*, i.e. collection of data divided into rows and columns.
The rule named ``?`` is the *entry* to the query,
and the relation it represents is the result of the query.
Each named rule has a rule head, which corresponds to the columns of the relation,
and a rule body, which specifies the content of the relation, or how the content should be computed.

Relations in Cozo (stored or otherwise) abide by the *set semantics*.
Thus even if a rule computes a row multiple times,
the resulting relation only contains a single copy.

There are two types of named rules in CozoScript:

* *Inline rules*, distinguished by using ``:=`` to connect the head and the body.
  The logic used to compute the resulting relation is defined *inline*.
* *Fixed rules*, distinguished by using ``<~`` to connect the head and the body.
  The logic used to compute the resulting relation is *fixed* according to which algorithm or utility is requested.

The *constant rules* which use ``<-`` to connect the head and the body are syntax sugar. For example::

    const_rule[a, b, c] <- [[1, 2, 3], [4, 5, 6]]

is identical to::

    const_rule[a, b, c] <~ Constant(data: [[1, 2, 3], [4, 5, 6]])

-----------------
Inline rules
-----------------

An example of an inline rule is::

    hc_rule[a, e] := rule_a['constant_string', b], rule_b[b, d, a, e]

The rule body of an inline rule consists of multiple *atoms* joined by commas,
and is interpreted as representing the *conjunction* of these atoms.

^^^^^^^^^^^^^^
Atoms
^^^^^^^^^^^^^^

Atoms come in various flavours.
In the example above::

    rule_a['constant_string', b]

is an atom representing a *rule application*: a rule named ``rule_a`` must exist in the same query
and have the correct arity (2 here).
Each row in the named rule is then *unified* with the bindings given as parameters in the square bracket:
here the first column is unified with a constant string, and unification succeeds only when the string
completely matches what is given;
the second column is unified with the *variable* ``b``,
and as the variable is fresh at this point (because this is its first appearance),
the unification will always succeed. For subsequent atoms, the variable becomes *bound*:
it take on the value of whatever it was
unified with in the named relation.
When a bound variable is unified again, for example ``b`` in ``rule_b[b, d, a, e]``,
this unification will only succeed when the unified value is the same as the current value.
Thus, repeated use of the same variable in named rules corresponds to inner joins in relational algebra.

Atoms representing applications of *stored relations* are written as::

    *stored_relation[bind1, bind2]

with the asterisk before the name.
Written in this way using square brackets, as many bindings as the arity of the stored relation must be given.

You can also bind columns by name::

    *stored_relation{col1: bind1, col2: bind2}

In this form, any number of columns may be omitted.
If the name you want to give the binding is the same as the name of the column, you can write instead
``*stored_relation{col1}``, which is the same as ``*stored_relation{col1: col1}``.

*Expressions* are also atoms, such as::

    a > b + 1

``a`` and ``b`` must be bound somewhere else in the rule. Expression atoms must evaluate to booleans,
and act as *filters*. Only rows where the expression atom evaluates to ``true`` are kept.

*Unification atoms* unify explicitly::

    a = b + c + d

Whatever appears on the left-hand side must be a single variable and is unified with the result of the right-hand side.

.. NOTE::
    This is different from the equality operator ``==``,
    where the left-hand side is a completely bound expression.
    When the left-hand side is a single *bound* variable,
    the equality and the unification operators are equivalent.

*Unification atoms* can also unify with multiple values in a list::

    a in [x, y, z]

If the right-hand side does not evaluate to a list, an error is raised.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Head
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

As explained above, Atoms correspond to either relations, projections or filters in relational algebra.
Linked by commas, they therefore represent a joined relation, with columns either constants or variables.
The *head* of the rule, which in the simplest case is just a list of variables,
then defines the columns to keep in the output relation and their order.

Each variable in the head must be bound in the body (the *safety rule*).
Not all variables appearing in the body need to appear in the head.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Multiple definitions and disjunction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For inline rules only, multiple rule definitions may share the same name,
with the requirement that the arity of the head in each definition must match.
The returned relation is then formed by the *disjunction* of the multiple definitions (a *union* of rows).

You may also use the explicit disjunction operator ``or`` in a single rule definition::

    rule1[a, b] := rule2[a] or rule3[a], rule4[a, b]

There is also an ``and`` operator, semantically identical to the comma ``,``
but has higher operator precedence than ``or`` (the comma has the lowest precedence).

^^^^^^^^^^^^^^^^
Negation
^^^^^^^^^^^^^^^^

Atoms in inline rules may be *negated* by putting ``not`` in front of them::

    not rule1[a, b]

When negating rule applications and stored relations,
at least one binding must be bound somewhere else in the rule in a non-negated context (another *safety rule*).
The unbound bindings in negated rules remain unbound: negation cannot introduce new bindings to be used in the head.

Negated expressions act as negative filters,
which is semantically equivalent to putting ``!`` in front of the expression.
Explict unification cannot be negated unless the left-hand side is bound,
in which case it is treated as an expression atom and then negated.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Recursion and stratification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The body of an inline rule may contain rule applications of itself,
and multiple inline rules may apply each other recursively.
The only exception is the entry rule ``?``, which cannot be referred to by other rules including itself.

Recursion cannot occur in negated positions (*safety rule*): ``r[a] := not r[a]`` is not allowed.

.. WARNING::
    As CozoScript allows explicit unification,
    queries that produce infinite relations may be accepted by the compiler.
    One of the simplest examples is::

        r[a] := a = 0
        r[a] := r[b], a = b + 1
        ?[a] := r[a]

    It is not even in principle possible for Cozo to rule out all infinite queries without wrongly rejecting valid ones.
    If you accidentally submitted one, refer to the system ops chapter for how to terminate queries.
    Alternatively, you can give a timeout for the query when you submit.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Aggregation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In CozoScript, aggregations are specified for inline rules by applying *aggregation operators* to variables
in the rule head::

    ?[department, count(employee)] := *personnel{department, employee}

here we have used the familiar ``count`` operator.
Any variables in the head without aggregation operators are treated as *grouping variables*,
and aggregation is applied using them as keys.
If you do not specify any grouping variables, then the resulting relation contains at most one row.

Aggregation operators are applied to the rows computed by the body of the rule using bag semantics.
The reason for this complication is that if aggregations are applied with set semantics, then the following query::

    ?[count(employee)] := *personnel{employee}

does not do what you expect: it either returns a row with a single value ``1`` if there are any matching rows,
or it returns nothing at all if the stored relation is empty.

If a rule has several definitions, they must have identical aggregations applied in the same positions.

Cozo allows aggregations for self-recursion for a limited subset of aggregation operators,
the so-called *semi-lattice aggregations*::

    shortest_distance[destination, min(distance)] :=
        route{source: 'A', destination, distance}

    shortest_distance[destination, min(distance)] :=
        shortest_distance[existing_node, prev_distance], # recursion
        route{source: existing_node, distance: route_distance},
        distance = prev_distance + route_distance

    ?[destination, min_distance] :=
        shortest_distance[destination, min_distance]

Here self-recursion of ``shortest_distance`` contains the ``min`` aggregation.

----------------------------------
Fixed rules
----------------------------------

The body of a fixed rule starts with the name of the utility or algorithm being applied,
then takes a specified number of named or stored relations as its *input relations*,
followed by *options* that you provide.
For example::

    ?[] <~ PageRank(*route[], theta: 0.5)

In the above example, the relation ``*route`` is the single input relation expected.
Input relations may be stored relations or relations resulting from rules.

Each utility/algorithm expects specific shapes for their input relations.
You must consult the documentation for each utility/algorithm to understand its API.

In fixed rules, bindings for input relations are usually omitted, but sometimes if they are provided
they are interpreted and used in algorithm-specific ways, for example in the DFS algorithm bindings.

In the example above, ``theta`` is an option of the algorithm,
which is required by the API to be an expression evaluating to a constant.
Each utility/algorithm expects specific types for the options;
some options have default values and may be omitted.

Each fixed rule has a determinate output arity.
Thus, the bindings in the rule head can be omitted,
but if they are provided, you must abide by the arity.

-----------------------
Query options
-----------------------

Each query can have options associated with it::

    ?[name] := *personnel{name}

    :limit 10
    :offset 20

In the example, ``:limit`` and ``:offset`` are query options with familiar meanings.
All query options start with a single colon ``:``.
Queries options can appear before or after rules, or even sandwiched between rules.

Several query options deal with transactions for the database.
Those will be discussed in the chapter on stored relations and transactions.
The rest of the query options are explained in the following.

.. module:: QueryOp
    :noindex:

.. function:: :limit <N>

    Limit output relation to at most ``<N>`` rows.
    If possible, execution will stop as soon as this number of output rows is collected.

.. function:: :offset <N>

    Skip the first ``<N>`` rows of the returned relation.

.. function:: :timeout <N>

    Abort if the query does not complete within ``<N>`` seconds.
    Seconds may be specified as an expression so that random timeouts are possible.

.. function:: :sleep <N>

    If specified, the query will wait for ``<N>`` seconds after completion,
    before committing or proceeding to the next query.
    Seconds may be specified as an expression so that random timeouts are possible.
    Useful for deliberately interleaving concurrent queries to test complex logic.

.. function:: :sort <SORT_ARG> (, <SORT_ARG>)*

    Sort the output relation. If ``:limit`` or ``:offset`` are specified, they are applied after ``:sort``.
    Specify ``<SORT_ARG>`` as they appear in the rule head of the entry, separated by commas.
    You can optionally specify the sort direction of each argument by prefixing them with ``+`` or ``-``,
    with minus denoting descending order, e.g. ``:sort -count(employee), dept_name``
    sorts by employee count in reverse order first,
    then break ties with department name in ascending alphabetical order.

    .. WARNING::
        Aggregations must be done in inline rules, not in output sorting. In the above example,
        the entry rule head must contain ``count(employee)``, ``employee`` alone is not acceptable.

.. function:: :order <SORT_ARG> (, <SORT_ARG>)*

    Alias for ``:sort``.

.. function:: :assert none

    The query returns nothing if the output relation is empty, otherwise execution aborts with an error.
    Useful for transactions and triggers.

.. function:: :assert some

    The query returns nothing if the output relation contains at least one row,
    otherwise, execution aborts with an error.
    Useful for transactions and triggers.
    You should consider adding ``:limit 1`` to the query to ensure early termination
    if you do not need to check all return tuples.