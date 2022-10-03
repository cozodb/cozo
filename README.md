# The Cozo Database

Cozo is an experimental, relational database that has a focus on graph data, with support for ACID transactions. It aims to implement a Purer, Better relational algebra without the historical baggage of SQL.

## Teasers

We have stored in our database a relation containing air travel routes. The following query uses joins to find airports reachable by one stop from Frankfurt Airport (FRA), the busiest airport in the world:

```
?[destination] := :route{src: 'FRA', dst: stop}, 
                  :route{src: stop, dst: destination}
```

Using recursion and inline rules, we can find _all_ airports reachable from Frankfurt (the transitive closure):

```
reachable[airport] := :route{src: 'FRA', dst: airport}
reachable[airport] := reachable[stop], :route{src: stop, dst: airport}
?[airport] := reachable[airport]
```

With aggregation and unification, we can compute the shortest path between Frankfurt and all airports in the world:

```
shortest_paths[dst, shortest(path)] := :route{src: 'FRA', dst},
                                       path = ['FRA', dst]
shortest_paths[dst, shortest(path)] := shortest_paths[stop, prev_path], 
                                       :route{src: stop, dst},
                                       path = append(prev_path, dst)
?[dst, path] := shortest_paths[dst, path]
```

The above computation is asymptotically optimal. For common operations on graphs like the shortest path, using built-in stock algorithms is both simpler and can further boost performance:

```
starting[airport] := airport = 'FRA'
?[src, dst, cost, path] <~ ShortestPathDijkstra(:route[], starting[])
```

Cozo is capable of much more. Follow the Tutorial to get started.

## Learning

* The Tutorial will get you started with running Cozo and learning the query language CozoScript.
* The Manual tries to document everything Cozo currently has to offer. It goes into greater depths for some topics that are glossed over in the Tutorial.

## Motivation

The so-called "NoSQL" movement in recent years brought forth a plethora of databases that try to introduce new data paradigms and revolutionize the industry. However, almost all the so-called "new" paradigms, in particular, the document paradigm, the (entity-relationship) graph model paradigm, and the key-value paradigm, actually predate the invention of the relational model. There is nothing wrong _per se_ with recycling old ideas, as changing circumstances can make previously infeasible solutions viable. However, since the historical development is deliberately obscured (with understandable business motivations), many users and even implementers fail to understand why relational databases became the standard in the first place, and do not have a clear picture of the strengths and weaknesses of the new databases. Suboptimal systems result. It is inevitable but still mildly amusing that even the name "NoSQL" was later reinterpreted to become "Not Only SQL".

So what is essential about these relational databases that has earned them such a firm position in the industry? Looking at the history of ideas accompanying the emergence of the relational systems, the answer is obvious: relational algebra. This intuitive, idealized mathematical model of data is powerful and elegant because it is an _algebra_, in particular, because it has the _closure property_ of algebras: operations on relations still produce relations. Thus, relations become a generic interface for data: once stored in the relational form, the data can be subjected to _all_ of the allowed transformations, and these can be nested or even applied recursively. An important consequence of this power and flexibility is that you do not need to foresee every eventual use of the data and only need to store data in a canonical, business-logic-agnostic form (think of the "normal forms" and all the theory behind them). Of course, in real situations it is impossible to uphold this principle in every case, mainly due to performance constraints, but that's the general spirit of relational databases: any data that you care to put into your persistent storage are probably going to outlive current your business logic by a huge margin.

But the NoSQL movement did occur, and with good reasons: relational databases fail in some ways. Every person has perhaps their own list of perceived shortcomings of relational databases, such as (the old relational systems') inability of dealing with the Big Data that comes with the explosion of the Internet. One of them is particularly unfortunate, however: the claim that relational databases are just bad with graph data. This accusation is particularly acute in the age of social networks. However, "graphs", "networks" and "relationships" are kind of synonyms, and "relational" is even in the name of relational algebra! In fact, relational algebra itself is perfectly capable of dealing with graph structures, and with recursion introduced, traditional relational databases can be no less powerful than dedicated graph databases.

If relational algebra itself is not a real obstacle, why are many graph databases "going beyond" it, and in the process throwing away the closure property, which in practice makes the data stored much harder to use beyond the business logic originally envisioned? We think SQL is to blame. The syntax is kind of backward (it really logically should be "FROM-WHERE-SELECT" rather than the traditional "SELECT-FROM-WHERE", both humans and auto-completions have to mentally reorder as a consequence), inline nesting is hard to read and has corner cases (certain types of "correlated queries" which in fact cannot be expressed in relational algebra), common table expressions are clunky and escalate quickly to unreadability when recursion is thrown in. And nesting, joins, and recursion are essential for graphs. In this day, using SQL for querying graphs feels like using FORTRAN for scripting webpages.

Datalog is a solution ...

Commercial systems are averse to breaking SQL compatibility ...

## Another database?!

Every few days a new database comes out and is advertised to be the Next Big Thing. This presents difficulties for users who try to decide which to use for the next project ... well, we actually didn't have any such difficulty 90% of the time: just stick to sqlite or postgres[^1]! For the remaining 10%, though, we are troubled by heavy joins that are too complicated to read, recursive CTEs that are a total pain to write, or mysterious query (anti-)optimizations that require a PhD degree to debug. And these invariably happen when we try to process our data mainly as networks, not tables.

[^1]: Or cassandra if the data is really too big, but cassandra is not as nice to use.

Yeah, we know there are graph databases designed just for this use case. We've used dozens of them at various stages. Some of them use syntax that is an improvement over SQL for simple graph cases but is actually not substantially more expressive for complicated situations. Some of them are super powerful but require you to write semi-imperative code. A few of them are "multi-paradigm" and attempt to support different logical data models simultaneously, with the result that none was supported very well. So we are not very satisfied.

## Goals and non-goals

In a sense, Cozo is our ongoing experiment for building a database that is powerful, reliable and at the same time a joy to use.

* Clean syntax, easy to write and read, even when dealing with convoluted problems.
* Well-defined semantics, even when dealing with recursions.
* Efficient short-cuts for common graph queries.
* No elaborate set-up necessary for ad-hoc queries and explorations.
* Integrity and consistency of data when explicitly required.

At the moment, the following are non-goals (but may change in the future)

* Sharding, distributed system. This would greatly increase the complexity of the system, and we don't think it is appropriate to pour energy into it at this experimentation stage.
* Additional query languages, e.g. SQL, GraphQL.
* Support for more paradigms, e.g. a document store. many a date
