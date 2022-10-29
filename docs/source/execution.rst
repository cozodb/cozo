====================================
Query execution
====================================

Databases often consider how queries are executed an implementation detail
hidden behind an abstraction barrier that users need not care about,
so that databases can utilize query optimizers to choose the best query execution plan
regardless of how the query was originally written.
This abstraction barrier is leaky, however,
since bad query execution plans invariably occur,
and users need to "reach behind the curtain" to fix performance problems,
which is a difficult and tiring task.
The problem becomes more severe the more joins a query contains,
and graph queries tend to contain a large number of joins.

So in Cozo we take the pragmatic approach and make query execution deterministic
and easy to tell from how the query was written.
The flip side is that we demand the user to
know what is the best way to store their data,
which is in general less demanding than coercing the query optimizer.
Then, armed with knowledge of this chapter, writing efficient queries is easy.

--------------------------------------
Disjunctive normal form
--------------------------------------

Evaluation starts by canonicalizing inline rules into
`disjunction normal form <https://en.wikipedia.org/wiki/Disjunctive_normal_form>`_,
i.e., a disjunction of conjunctions, with any negation pushed to the innermost level.
Each clause of the outmost disjunction is then treated as a separate rule.
The consequence is that the safety rule may be violated
even though textually every variable in the head occurs in the body.
As an example::

    rule[a, b] := rule1[a] or rule2[b]

is a violation of the safety rule since it is rewritten into two rules, each of which is missing a different binding.

--------------------------------------
Stratification
--------------------------------------

The next step in the processing is *stratification*.
It begins by making a graph of the named rules,
with the rules themselves as nodes, 
and a link is added between two nodes when one of the rules applies the other.
This application is through atoms for inline rules, and input relations for fixed rules.

Next, some of the links are labelled *stratifying*:

* when an inline rule applies another rule through negation,
* when an inline rule applies another inline rule that contains aggregations,
* when an inline rule applies itself and it has non-semi-lattice,
* when an inline rule applies another rule which is a fixed rule,
* when a fixed rule has another rule as an input relation.

The strongly connected components of the graph of rules are then determined and tested,
and if it found that some strongly connected component contains a stratifying link,
the graph is deemed *unstratifiable*, and the execution aborts.
Otherwise, Cozo will topologically sort the strongly connected components to
determine the strata of the rules:
rules within the same stratum are logically executed together,
and no two rules within the same stratum can have a stratifying link between them.
In this process, 
Cozo will merge the strongly connected components into as few supernodes as possible
while still maintaining the restriction on stratifying links.
The resulting strata are then passed on to be processed in the next step.

You can see the stratum number assigned to rules by using the ``::explain`` system op.

--------------------------------------
Magic set rewrites
--------------------------------------

Within each stratum, the input rules are rewritten using the technique of *magic sets*.
This rewriting ensures that the query execution does not
waste time calculating results that are later simply discarded.
As an example, consider::

    reachable[a, b] := link[a, n]
    reachable[a, b] := reachable[a, c], link[c, b]
    ?[r] := reachable['A', r]

Without magic set rewrites, the whole ``reachable`` relation is generated first, 
then most of them are thrown away, keeping only those starting from ``'A'``.
Magic set rewriting avoids this problem.
You can see the result of the rewriting using ``::explain``.
The rewritten query is guaranteed to yield the same relation for ``?``,
and will in general yield fewer intermediate rows.

The rewrite currently only applies to inline rules without aggregations.

--------------------------------------
Semi-naïve evaluation
--------------------------------------

Now each stratum contains either a single fixed rule or a set of inline rules.
The single fixed rules are executed by running their specific implementations.
For the inline rules, each of them is assigned an output relation.
Assuming we know how to evaluate each rule given all the relations it depends on, 
the semi-naïve algorithm can now be applied to the rules to yield all output rows.

The semi-naïve algorithm is a bottom-up evaluation strategy, meaning that it tries to deduce
all facts from a set of given facts.

.. NOTE::
    By contrast, top-down strategies start with stated goals and try to find proof for the goals.
    Bottom-up strategies have many advantages over top-down ones when the whole output of each rule
    is needed, but may waste time generating unused facts if only some of the output is kept.
    Magic set rewrites are introduced to eliminate precisely this weakness.

---------------------------------------
Ordering of atoms
---------------------------------------

The compiler reorders the atoms in the body of the inline rules, and then
the atoms are evaluated.

After conversion to disjunctive normal forms,
each atom can only be one of the following:

* an explicit unification,
* applying a rule or a stored relation,
* an expression, which should evaluate to a boolean,
* a negation of an application.

The first two cases may introduce fresh bindings, whereas the last two cannot. 
The reordering make all atoms that introduce new bindings stay where they are,
whereas all atoms that do not introduce new bindings are moved to the earliest possible place
where all their bindings are bound.
All atoms that introduce bindings correspond to
joining with a pre-existing relation followed by projections
in relational algebra, and all atoms that do not correspond to filters. 
By applying filters as early as possible,
we minimize the number of rows before joining them with the next relation.

When writing the body of rules, we should aim to minimize the total number of rows generated.
A strategy that works almost in all cases is to put the most restrictive atoms which generate new bindings first.

---------------------------------------
Evaluating atoms
---------------------------------------

We now explain how a single atom which generates new bindings is processed.

For unifications, the right-hand side, an expression with all variables bound,
is simply evaluated, and the result is joined
to the current relation (as in a ``map-cat`` operation in functional languages).

Rules or stored relations are conceptually trees, with composite keys sorted lexicographically.
The complexity of their applications in atoms
is therefore determined by whether the bound variables and constants in the application bindings form a *key prefix*.
For example, the following application::

    a_rule['A', 'B', c]

with ``c`` unbound, is very efficient, since this corresponds to a prefix scan in the tree with the key prefix ``['A', 'B']``,
whereas the following application::

    a_rule[a, 'B', 'C']

where ``a`` is unbound, is very expensive, since we must do a full scan.
On the other hand, if ``a`` is bound, then this is only a logarithmic-time existence check.

For stored relations, you need to check its schema for the order of keys to deduce the complexity.
The system op ``::explain`` may also give you some information.

Rows are generated in a streaming fashion,
meaning that relation joins proceed as soon as one row is available,
and do not wait until the whole relation is generated.

---------------------------------------
Early stopping
---------------------------------------

For the entry rule ``?``, if ``:limit`` is specified as a query option,
a counter is used to monitor how many valid rows are already generated.
If enough rows are generated, the query stops. 
This only works when the entry rule is inline
and you do not specify ``:order``.