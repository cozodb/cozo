====================================
Query execution
====================================

Usually, in a database,
how queries are executed is usually considered an implementation detail
hidden behind an abstraction barrier, which normal users need not care about.
The idea is that databases will take advantage of this abstraction barrier
by using query optimizers to choose the best query plan,
regardless of how the query was originally written.
As everyone knows, however, this abstraction barrier is leaky,
since bad query execution plans invariably occur and hurt performance.
The problem is especially severe when dealing with graphs, 
since graph traversals generally use a lot more joins than non-graph queries,
and the reliability of even the best query optimizer 
decreases exponentially with the number of joins.

In Cozo we take the pragmatic approach and assume that the user eventually
knows (or should know) what is the best way to query their data.
This is certainly true for those developers who spend hours
"coercing" the query optimizers to use a query plan that the user intends,
sometimes in rather convoluted ways.
In Cozo, no coercion is necessary since the query execution is completely
determined by how the query is written: 
there is no stats-based query planning involved.
In our experience, this saves quite a lot of developer time,
since developers eventually learn how to write efficient queries naturally, 
and after they do, they no longer have to deal with endless "query de-optimizations".

--------------------------------------
Stratification
--------------------------------------

As discussed in the chapter on queries, Cozo sees a query as a set of named rules.
Fixed rules are left as they are, 
and inline rules are converted into disjunctive normal forms.
After conversion, all inline rules consist of conjunction of atoms only,
and negation only occurs for the leaf atoms.

The next step towards executing the query is *stratifying* the rules.
Stratification begins by making a graph of the named rules, 
with the rules themselves as nodes, 
and a link is added between two nodes when one of the rules applies the other.
This application is through atoms for inline rules, and input relations for fixed rules.
Now some of the links are labelled *stratifying*: 
when an inline rule applies another rule through negation,
when an inline rule applies another inline rule that contains aggregations,
when an inline rule applies itself and it has non-semi-lattice,
when an inline rule applies another rule which is a fixed rule,
or when a fixed rule has another rule as an input relation.
The strongly connected components of this graph are then determined and tested, 
and if it found that some strongly connected component contains a stratifying link,
the graph is deemed *unstratifiable*, and the execution aborts.
Otherwise, Cozo will topologically sort the strongly connected components to
determine a *stratification* of the rules: 
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
Within each stratum, the input rules are rewritten using a technique called magic sets.
In intuitive terms, this rewriting is to ensure that the query execution does not
waste time calculating results that are then simply discarded. 
As an example, consider::

    reachable[a, b] := link[a, n]
    reachable[a, b] := reachable[a, c], link[c, b]
    ?[r] := reachable['A', r]

Without magic set rewrites, the whole ``reachable`` relation is generated first, 
then most of them are thrown away, keeping only those starting from ``'A'``.
Magic set avoids this problem. How the rewrite proceeds is rather technical,
but you can see the results in the output of ``::explain``.

The rewritten query is guaranteed to yield the same relation for ``?``, 
and will in general yield fewer intermediate rows.

Currently, the rewrite applies only to inline rules without aggregations.
So for the moment being, you may need to manually constrain some of your rules.

--------------------------------------
Semi-naïve evaluation
--------------------------------------

Now each stratum contains either a single fixed rule or a set of inline rules.
The single fixed rule case is easy: just run the specific implementation of the rule.
In the case of the inline rules, each of the rules is assigned an output relation.
Assuming we know how to evaluate each rule given all the relations it depends on, 
the semi-naïve algorithm can now be applied to the rules to yield all output rows.

The semi-naïve algorithm is a bottom-up evaluation strategy, meaning that it tries to deduce
all facts from a set of given facts.
By contrast, top-down strategies start with stated goals and try to find proof for the goals. 
Bottom-up strategies have many advantages over top-down ones when the whole output of each rule
is needed, but may waste time generating unused facts if only some of the output is kept.
Magic set rewrites are introduced to eliminate precisely this weakness.


---------------------------------------
Ordering of atoms
---------------------------------------

Now we discuss how a single definition of an inline rule is evaluated. 
We know from the query chapter that the body of the rule contains atoms,
and after conversion to disjunctive normal forms, all atoms are linked by conjunction,
and each atom can only be one of the following:

* an explicit unification,
* applying a rule or a stored relation,
* an expression, which should evaluate to a boolean,
* a negation of an application.

The first two cases may introduce fresh bindings, whereas the last two cannot. 
The atoms are then reordered: all atoms that introduce new bindings stay where they are,
whereas all atoms that do not introduce new bindings are moved to the earliest possible place
where all their bindings are bound. In fact, 
all atoms that introduce bindings correspond to 
joining with a pre-existing relation followed by projections
in relational algebra, and all atoms that do not correspond to filters. 
The idea is to apply filters as early as possible 
to minimize the number of rows before joining with the next relation.

This procedure is completely deterministic. 
When writing the body of rules, we therefore should aim to minimize the total number of rows generated.
A strategy that works almost in all cases is to put the most restrictive atoms which generate new bindings first,
as this can make the left relation in each join small.

---------------------------------------
Relations as indices
---------------------------------------

Next, we need to understand how a single atom which generates new bindings is processed.

For the case of unification, it is simple: the right-hand side of the unification, 
which is an expression with all variables bound, is simply evaluated, and the result is joined
to the current relation (as in a ``map-cat`` operation in functional languages).

For the case of the application of relations, 
the first thing to understand is that all relations in Cozo are conceptually trees.
All the bindings of relations generated by inline or fixed rules, 
and the keys of stored relations, act as a composite key for the tree.
The access complexity is therefore determined by whether a key component is bound.
For example, the following application::

    a_rule['A', 'B', c]

with ``c`` unbound is very efficient, since this corresponds to a prefix scan in the tree with the key prefix ``['A', 'B']``,
whereas the following application::

    a_rule[a, 'B', 'C']

where ``a`` is unbound is very expensive, since we must do a full relation scan. 
On the other hand, if ``a`` is bound, then this is only a logarithmic-time check.

For stored relations, you need to check its schema for the order of keys to deduce the complexity.
The system op ``::explain`` may also give you some information.

---------------------------------------
Early stopping
---------------------------------------

Within each stratum, rows are generated in a streaming fashion.
For the entry rule ``?``, if ``:limit`` is specified as a query option, 
a counter is used to monitor how many valid rows are already generated.
If enough rows are generated, the query stops. 
Note that this only works when the entry rule is inline, 
and when you are *not* specifying ``:order``.