==============
Queries
==============

The Cozo database system is queried using the CozoScript language.
At its core, CozoScript is a `Datalog <https://en.wikipedia.org/wiki/Datalog>`_ dialect
supporting stratified negation and stratified recursive meet-aggregations.
The built-in native algorithms (mainly graph algorithms) further empower
CozoScript for much greater ease of use and much wider applicability.

A query consists of one or many named rules.
Each named rule conceptually represents a relation or a table with rows and columns.
The rule named ``?`` is called the entry to the query,
and its associated relation is returned as the result of the query.
Each named rule has associated with it a rule head, which names the columns of the relation,
and a rule body, which specifies the content of the relation, or how the content should be computed.

In CozoScript, relations (stored relations or relations defined by rules) abide by the *set semantics*,
meaning that even if a rule may compute a row multiple times, it will occur only once in the output.
This is in contradistinction to SQL.

There are three types of named rules in CozoScript: constant rules, Horn-clause rules and algorithm applications.

-----------------
Constant rules
-----------------

The following is an example of a constant rule::

    const_rule[a, b, c] <- [[1, 2, 3], [4, 5, 6]]

Constant rules are distinguished by the symbol ``<-`` separating the rule head and rule body.
The rule body should be an expression evaluating to a list of lists:
every subslist of the rule body should be of the same length (the *arity* of the rule),
and must match the number of arguments in the rule head.
In general, if you are passing data into the query,
you should take advantage of named parameters::

    const_rule[a, b, c] <- $data_passed_in

and pass a map containing a key of ``"data_passed_in"`` with a value of a list of lists.

The rule head may be omitted if the rule body is not the empty list::

    const_rule[] <- [[1, 2, 3], [4, 5, 6]]

in which case the system will deduce the arity of the rule from the data.

-----------------
Horn-clause rules
-----------------

An example of a Horn-clause rule is::

    hc_rule[a, e] := rule_a['constant_string', b], rule_b[b, d, a, e]

As can be seen, Horn-clause rules are distinguished by the symbol ``:=`` separating the rule head and rule body.
The rule body of a Horn-clause rule consists of multiple *atoms* joined by commas,
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
and as the variable is fresh at this point (meaning that it first appears here),
the unification will always succeed and the variable will become *bound*:
from this point take on the value of whatever it was
unified with in the named relation.

When a bound variable is used again later, for example in ``rule_b[b, d, a, e]``, the variable ``b`` was bound
at this point, this unification will only succeed when the unified value is the same as the previously unified value.
In other words, repeated use of the same variable in named rules corresponds to inner joins in relational algebra.

Another flavour of atoms is the *stored relation*. It may be written similarly to a rule application::

    :stored_relation[bind1, bind2]

with the colon in front of the stored relation name to distinguish it from rule application.
Written in this way, you must give as many bindings to the stored relation as its arity,
and the bindings proceed by argument positions, which may be cumbersome and error-prone.
So alternatively, you may use the fact that columns of a stored relation are always named and bind by name::

    :stored_relation{col1: bind1, col2: bind2}

In this case, you only need to bind as many variables as you use.
If the name you want to give the binding is the same as the name of the column, you may use the shorthand notation:
``:stored_relation{col1}`` is the same as ``:stored_relation{col1: col1}``.

*Expressions* are also atoms, such as::

    a > b + 1

Here ``a`` and ``b`` must be bound somewhere else in the rule, and the expression must evaluate to a boolean, and act as a *filter*: only rows where the expression evaluates to true are kept.

You can also use *unification atoms* to unify explicitly::

    a = b + c + d

for such atoms, whatever appears on the left-hand side must be a single variable and is unified with the right-hand side.
This is different from the equality operator ``==``,
where both sides are merely required to be expressions.
When the left-hand side is a single *bound* variable,
it may be shown that the equality and the unification operators are semantically equivalent.

Another form of *unification atom* is the explicit multi-unification::

    a in [x, y, z]

here the variable on the left-hand side of ``in`` is unified with each item on the right-hand side in turn,
which in turn implies that the right-hand side must evaluate to a list
(but may be represented by a single variable or a function call).

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Head and returned relation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Atoms, as explained above, corresponds to either relations (or their projections) or filters in relational algebra.
Linked by commas, they, therefore, represent a joined relation, with named columns.
The *head* of the rule, which in the simplest case is just a list of variables,
then defines whichever columns to keep, and their order in the output relation.

Each variable in the head must be bound in the body, this is one of the *safety rules* of Datalog.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Multiple definitions and disjunction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For Horn-clause rules only, multiple rule definitions may share the same name,
with the requirement that the arity of the head in each definition must match.
The returned relation is then the *disjunction* of the multiple definitions,
which correspond to *union* in SQL.
*Intersect* in SQL can be written in CozoScript into a single rule since commas denote conjunction.
In complicated situations, you may instead write disjunctions in a single rule with the explicit ``or`` operator::

    rule1[a, b] := rule2[a] or rule3[a], rule4[a, b]

For completeness, there is also an explicit ``and`` operator, but it is semantically identical to the comma, except that
it has higher operator precedence than ``or``, which in turn has higher operator precedence than the comma.

During evaluation, each rule is canonicalized into disjunction normal form
and each clause of the outmost disjunction is treated as a separate rule.
The consequence is that the safety rule may be violated
even though textually every variable in the head occurs in the body.
As an example::

    rule[a, b] := rule1[a] or rule2[b]

is a violation of the safety rule since it is rewritten into two rules, each of which is missing a different binding.

^^^^^^^^^^^^^^^^
Negation
^^^^^^^^^^^^^^^^

Atoms in Horn clauses may be *negated* by putting ``not`` in front of them, as in::

    not rule1[a, b]

When negating rule applications and stored relations,
at least one binding must be bound somewhere else in the rule in a non-negated context:
this is another safety rule of Datalog, and it ensures that the outputs of rules are always finite.
The unbound bindings in negated rules remain unbound: negation cannot introduce bound bindings to be used in the head.

Negated expressions act as negative filters,
which is semantically equivalent to putting ``!`` in front of the expression.
Since negation does not introduce new bindings,
unifications and multi-unifications are converted to equivalent expressions and then negated.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Recursion and stratification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The body of a Horn-clause rule may contain rule applications of itself,
and multiple Horn-clause rules may apply each other recursively.
The only exception is the entry rule ``?``, which cannot be referred to by other rules.

Self and mutual references allow recursion to be defined easily. To guard against semantically pathological cases,
recursion cannot occur in negated positions: the Russell-style rule ``r[a] := not r[a]`` is not allowed.
This requirement creates an ordering of the rules, since
negated rules must evaluate to completion before rules that apply them can start evaluation:
this is called *stratification* of the rules.
In cases where a total ordering cannot be defined since there exists a loop in the ordering
required by negation, the query is then deemed unstratifiable and Cozo will refuse to execute it.

Note that since CozoScript allows unifying fresh variables, you can still easily write programs that produce
infinite relations and hence cannot complete through recursion, but that are still accepted by the database.
One of the simplest examples is::

    r[a] := a = 0
    r[a] := r[b], a = b + 1
    ?[a] := r[a]

It is up to the user to ensure that such programs are not submitted to the database,
as it is not even in principle possible for the database to rule out such cases without wrongly rejecting valid queries.
If you accidentally submitted one, you can refer to the system ops section for how to terminate long-running queries.
Or you can give a timeout for the query when you submit.

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Aggregation
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

CozoScript supports aggregations, as does SQL, which provides a very useful extension to pure relational algebra.
In CozoScript, aggregations are specified for Horn-clause rules by applying aggregation operators to variables 
in the rule head, as in::

    ?[department, count(employee)] := :personnel{department, employee}

here we have use the ``count`` operator familiar to all SQL users. 
The sementics is that any variables in the head without aggregation operators are treated as *grouping variables*,
similar to what appears in a ``GROUP BY`` clause in SQL, and the aggregation is applied using the grouping variables
as keys. If you do not specify any grouping variables, then you get at most one row as the return value.

As we now understand, CozoScript follows relational algebra with set semantics. 
With the introduction of aggregations, the situation is a little bit more complicated, 
as aggregations are applied to the relation resulting from the body of the rule using bag semantics,
and the resulting relation of the rule, after aggregations are applied, follow set semantics.
The reason for this complication is that if aggregations are applied with set semantics, then the following query::

    ?[count(employee)] := :personnel{employee}

does not do what you expect: it either returns a row with a single value ``1`` if there are any matching rows, 
or it returns nothing at all if the stored relation ``:personnel`` is empty. 
Though semantically sound, this behaviour is not useful at all.
So for aggregations we opt for bag semantics, and the query does what one expects.

If a rule has several definitions, they must have identical aggregations applied in the same positions,
otherwise the query will be rejected. 
The reason is that in complicated situations the sementics is ambiguous and counter-intuitive if we do allow it.

Existing database systems do not usually allow aggregations through recursion, 
since in many cases it is difficult to give a useful semantics to such queries.
In CozoScript we allow aggregations for self-recursion for a limited subset of aggregation operators, 
the so-called *meet aggregations*, such as the following example shows::

    shortest_distance[destination, min(distance)] := route{source: 'A', destination, distance}
    shortest_distance[destination, min(distance)] := 
        shortest_distance[existing_node, prev_distance], # recursion
        route{source: existing_node, distance: route_distance},
        distance = prev_distance + route_distance
    ?[destination, min_distance] := shortest_distance[destination, min_distance]

this query computes the shortest distances from a node to all nodes using the ``min`` aggregation operator.

With respect to stratification, if a rule has aggregations in its head, 
then any rule that contains it in an atom must be in a higher stratum, 
unless that rule is the same rule (self-recursion) and all aggregations in its head are meet aggregations.

For the aggregation operators available and more details of what "meet" aggregations mean, see the dedicated chapter.

----------------------------------
Algorithm application
----------------------------------

The final type of named rule, algorithm applications, is specified by the algorithm name, 
take a specified number of named or stored relations as inputs relations, and have specific options that you provide.
The following query is a calculation of PageRank::

    ?[] <~ PageRank(:route[], theta: 0.5)

Algorithm applications are distinguished by the curly arrow ``<~``.
In the above example, the relation ``:route`` is the single input relation expected.
Algorithms do not care if an input relation is stored or results from a rule.
Each algorithm expects specific shapes of input relations, 
for example, PageRank expects the first two columns of the relation to denote the source and destination
of links in a graph. You must consult the documentation for each algorithm to understand its API.
In algorithm applications, bindings for input relations are usually omitted, but sometimes if they are provided
they are interpreted and used in algorithm-specific ways, for example in the DFS algorithm bindings
can be used to construct a expression for testing the termination condition.
In the example, ``theta`` is a parameter of the algorithm, which is an expression evaluating to a constant.
Each algorithm expects specific types for parameters, and some parameters have default values and may be omitted.

Each algorithm has a determinate output arity. Usually you omit the bindings in the rule head, as we do above,
but if you do provide bindings, the arities must match.

In terms of stratification, each algorithm application lives in its own stratum: 
it is evaluated after all rules it depends on are completely evaluated, 
and all rules depending on the output relation of an algorithm starts evaluation only after complete evaluation
of the algorithm. In particular, unlike Horn-clause rules, there is no early termination even if the output relation
is for the entry rule.

-----------------------
Query options
-----------------------

Each query can have query options associated with it::

    ?[name] := :personnel{name}

    :limit 10
    :offset 20

In the example, ``:limit`` and ``:offset`` are query options, with familiar meaning from SQL. 
All query options starts with a single colon ``:``. 
Queries options can appear before or after rules, or even sandwiched between rules.
Use this freedom for best readability.

There are a number of query options that deal with transactions for the database. 
Those will be discussed in the chapter on stored relations and transactions. 
Here we explain query options that exclusively affects the query itself.

.. module:: QueryOp
    :noindex:

.. function:: :limit <N>

    Limit output relation to at most ``<N>`` rows. 
    If possible, execution will stop as soon as this number of output rows are collected.

.. function:: :offset <N>

    Skip the first ``<N>`` rows of the returned relation.

.. function:: :timeout <N>

    If the query does not complete within ``<N>`` seconds, abort.

.. function:: :sort SORT_ARG (, SORT_ARG)*

    Sort the output relation before applying other options or returnining. 
    Specify ``SORT_ARG`` as they appear in the rule head of the entry, separated by commas. 
    You can optionally specify the sort direction of each argument by prefixing them with ``+`` or ``-``,
    with minus denoting descending sort. As an example, ``:sort -count(employee), dept_name`` 
    sorts by employee count descendingly first, then break ties with department name in ascending alphabetical order.
    Note that your entry rule head must contain both ``dept_name`` and ``count(employee)``: 
    aggregations must be done in Horn-rules, not in output sorting.  ``:order`` is an alias for ``:sort``.

.. function:: :assert none

    With this option, the query returns nothing if the output relation is empty, otherwise execution aborts with an error.
    Essential for transactions and triggers.

.. function:: :assert some

    With this option, the query returns nothing if the output relation contains at least one row, 
    otherwise execution aborts with an error. 
    Execution of the query stops as soon as the first row is produced if possible.
    Essential for transactions and triggers.