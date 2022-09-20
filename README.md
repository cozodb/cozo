# The Cozo Database

Cozo is a graph-focused, dual-storage transactional database designed by and for data hackers. It's free and open-source software.

## Another database?!

Every few days a new database comes out and is advertised to be the Next Big Thing. This presents difficulties for users who try to decide which to use for the next project ... well, we actually didn't have any such difficulty 90% of the time: just stick to sqlite or postgres[^1]! For the remaining 10%, though, we are troubled by heavy joins that are too complicated to read, recursive CTEs that are a total pain to write, or mysterious query (anti-)optimizations that require a PhD degree to debug. And these invariably happen when we try to process our data mainly as networks, not tables.

[^1]: Or cassandra if the data is really too big, but cassandra is not as nice to use.

Yeah, we know there are graph databases designed just for this use case. We've used dozens of them at various stages. Some of them use syntax that is an improvement over SQL for simple graph cases but is actually not substantially more expressive for complicated situations. Some of them are super powerful but require you to write semi-imperative code. A few of them are "multi-paradigm" and attempt to support different logical data models simultaneously, with the result that none was supported very well. So we are not very satisfied.