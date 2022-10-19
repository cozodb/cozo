====================================
Query execution
====================================

Usually, in a database,
how queries are executed is usually considered an implementation detail
hidden behind an abstraction layer, which normal users need not care about.
As everyone knows, however, this abstraction layer is leaky,
since bad query execution plans invariably occur and hurt performance,
and developers routinely "go under" the abstraction layer to solve such problems.

Therefore, in Cozo we take the pragmatic approach and make certain guarantees
about query execution, which we will explain in the following.
It is essential to at least have a rough idea of these guarantees to write
efficient queries and to debug performance.

--------------------------------------
Stratification
--------------------------------------

--------------------------------------
Semi-naïve evaluation
--------------------------------------

--------------------------------------
Magic set rewrites
--------------------------------------

---------------------------------------
Relations as indices
---------------------------------------

---------------------------------------
Ordering of atoms
---------------------------------------

---------------------------------------
Early stopping
---------------------------------------