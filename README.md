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

If relational algebra itself is not a real obstacle, why are many graph databases "going beyond" it, and in the process throwing away the closure property, which in practice makes the data stored much harder to use beyond the business logic originally envisioned? We think SQL is to blame. The syntax is kind of backward (it really logically should be "FROM-WHERE-SELECT" rather than the traditional "SELECT-FROM-WHERE", both humans and auto-completions have to mentally reorder as a consequence), inline nesting is hard to read and has corner cases (certain types of "correlated queries" which in fact cannot be expressed in relational algebra), common table expressions are clunky and escalate quickly to unreadability when recursion is thrown in, and SQL actually differs from relational algebra in a fundamental way by adopting bag instead of set semantics, which is problematic for recursion. As nesting, joins, and recursion are essential for graphs but clumsy and not easy to use in SQL, in this day, using SQL for querying graphs feels like using FORTRAN for scripting webpages.

In fact, another much simpler query language has existed for quite some time (since 1986): Datalog, whose non-recursive part is equivalent to relational algebra. It is usually encountered when reading papers in relational database theories, where using SQL for mathematical reasoning is just too unwieldy. Most theoretical books on relational databases even have a chapter or section on Datalog, because it is so simple and "helps one to write SQL correctly". Not many databases support it directly though, a testimony of the fear of "breaking compatibility" and hence losing market. And those databases that support Datalog and available to the public certainly cannot be considered general-purpose databases.

This is where Cozo comes in. We want to prove, through a real database implementation, that relational model can be made much simpler and much more pleasant to use if we are prepared to ditch the SQL syntax. Furthermore, by combining the core relational algebra with recursion and aggregation (in a somewhat different way than usually done in SQL), we want to show that relational databases are perfectly capable of dealing with graphs efficiently, with a syntax that is both easy to write and easy to read and understand. How much we have succeeded is up to you, the user, to judge.

## Non-goals

* As Cozo is currently considered an experiment, it is probably not going to have distributed functions for quite some time, if ever.
* A feature in traditional RDBMS is the query optimizer. Cozo is not going to have one in the traditional sense for the moment, for two reasons. The first one is that building a good query optimizer takes enormous time, and at the moment we do not want to put our time in implementing one. The second, more fundamental reason is that, even with good query optimizers, like those in PostgreSQL, their usefulness in actually optimizing (instead of de-optimizing) queries decreases exponentially with the number of joins present. And graph queries tend to contain many more joins than non-graph queries. For complex queries, "debugging" the query plan is actually much harder than specifying the plan explicitly (which you cannot do in RDBMS, for some reason). In Cozo the execution order can be determined explicitly from how the query is written: there is no guesswork, and you do not play hide-and-seek with the query planner. We believe that the end user must understand the data sufficiently to efficiently use it, and even a superficial understanding allows one to write a reasonably efficient query. In our experience, the approach taken by traditional RDBMS is akin to a strongly typed programming language disallowing (or heavily discouraging) the programmer to write _any_ type declarations and insisting that all types must be inferred, thus giving its implementers an impossible task. When Cozo becomes more mature, we _may_ introduce query optimizers for limited situations in which they can have large benefits, but explicit specification will always remain an option.
* Cozo is not mature enough to benefit from elaborate account and security subsystems. Currently, Cozo has a required password authentication scheme with no defaults, but it is not considered sufficient for any purpose on the Internet. You should only run Cozo within your trusted network. The current security scheme is only meant to be a last counter-measure to the sorry situations of inadvertently exposing large swathes of data to the Internet.