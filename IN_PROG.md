[![GitHub](https://img.shields.io/github/license/cozodb/cozo)](https://github.com/cozodb/cozo/blob/main/LICENSE.txt)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/cozodb/cozo/Build)](https://github.com/cozodb/cozo/actions/workflows/build.yml)
[![Discord](https://img.shields.io/discord/1034464550927478886)](https://discord.gg/GFpjQ9m866)

# `cozo`

A general-purpose, transactional, relational database
that uses Datalog for query and focuses on graph data.

## Features

* Relational database with [Datalog](https://en.wikipedia.org/wiki/Datalog) as the query language
* Supports recursion, including recursion through (safe) aggregation, capable of expressing complex graph operations and algorithms
* Fixed rules providing efficient whole-graph algorithms which integrate seamlessly with Datalog
* Rich set of built-in functions and aggregations
* Only a single executable, trivial to deploy and run
* Easy to use from any programming language
* Special support for [Jupyter](https://jupyter.org/) notebooks, integrate nicely with the Python DataScience ecosystem 
* Modern, clean, flexible syntax, nice error messages

## Teasers

In the following, `*route` refers to a relation with two columns named `src` and `dst`, 
representing routes between airports.

Find airports reachable by one stop from Frankfurt Airport (code `FRA`, the busiest airport in the world):

```js
?[dst] := *route{src: 'FRA', dst: stop}, 
          *route{src: stop, dst}
```

Find _all_ airports reachable from Frankfurt (i.e., the transitive closure) 
with code starting with the letter `A`:

```js
reachable[dst] := *route{src: 'FRA', dst}
reachable[dst] := reachable[src], *route{src, dst}
?[airport] := reachable[airport], starts_with(airport, 'A')
```

Compute the shortest path between Frankfurt and all airports in the world with recursion through aggregation:

```js
shortest_paths[dst, shortest(path)] := *route{src: 'FRA', dst},
                                       path = ['FRA', dst]
shortest_paths[dst, shortest(path)] := shortest_paths[stop, prev_path], 
                                       *route{src: stop, dst},
                                       path = append(prev_path, dst)
?[dst, path] := shortest_paths[dst, path]
```

Use a fixed rule to compute the shortest path:

```js
starting[airport] := airport = 'FRA'
?[src, dst, cost, path] <~ ShortestPathDijkstra(*route[], starting[])
```

## Learning Cozo

* Start with the [Tutorial](https://cozodb.github.io/current/tutorial.html) to learn the basics.
* Continue with the [Manual](https://cozodb.github.io/current/manual/) to understand the fine points.


## Use cases

As Cozo is a general-purpose database,
it can be used in situations
where traditional databases such as PostgreSQL and SQLite
are used.
However, Cozo is designed to overcome several shortcomings
of traditional databases, and hence fares especially well
in specific situations:

* You have a lot of interconnected relations
  and the usual queries need to relate many relations together.
  In other words, you need to query a complex graph.
  * An example is a system granting permissions to users for specific tasks.
    In this case, users may have roles,
    belong to an organization hierarchy, and tasks similarly have organizations
    and special provisions associated with them.
    The granting process itself may also be a complicated rule encoded as data
    within the database.
  * With a traditional database,
    the corresponding SQL tend to become
    an entangled web of nested queries, with many tables joined together,
    and maybe even with some recursive CTE thrown in. This is hard to maintain,
    and worse, the performance is unpredictable since query optimizers in general
    fail when you have over twenty tables joined together.
  * With Cozo, on the other hand, Horn-clause rules make it easy to break
    the logic into smaller pieces and write clear, easily testable queries.
    Furthermore, the deterministic evaluation order makes identifying and solving
    performance problems easier.
* Your data may be simple, even a single table, but it is inherently a graph.
  * We have seen an example in the [Tutorial](https://cozodb.github.io/current/tutorial.html):
    the air route dataset, where the key relation contains the routes connecting airports.
  * In traditional databases, when you are given a new relation,
    you try to understand it by running aggregations on it to collect statistics:
    what is the distribution of values, how are the columns correlated, etc.
  * In Cozo you can do the same exploratory analysis,
    except now you also have graph algorithms that you can
    easily apply to understand things such as: what is the most _connected_ entity,
    how are the nodes connected, and what are the _communities_ structure within the nodes.
* Your data contains hidden structures that only become apparent when you
  identify the _scales_ of the relevant structures.
  * Examples are most real networks, such as social networks,
    which have a very rich hierarchy of structures
  * In a traditional database, you are limited to doing nested aggregations and filtering,
    i.e. a form of multifaceted data analysis. For example, you can analyze by gender, geography,
    job or combinations of them. For structures hidden in other ways,
    or if such categorizing tags are not already present in your data,
    you are out of luck.
  * With Cozo, you can now deal with emergent and fuzzy structures by using e.g.
    community detection algorithms, and collapse the original graph into a coarse-grained
    graph consisting of super-nodes and super-edges.
    The process can be iterated to gain insights into even higher-order emergent structures.
    This is possible in a social network with only edges and _no_ categorizing tags
    associated with nodes at all,
    and the discovered structures almost always have meanings correlated to real-world events and
    organizations, for example, forms of collusion and crime rings.
    Also, from a performance perspective,
    coarse-graining is a required step in analyzing the so-called big data,
    since many graph algorithms have high complexity and are only applicable to
    the coarse-grained small or medium networks.
* You want to understand your live business data better by augmenting it into a _knowledge graph_.
  * For example, your sales database contains product, buyer, inventory, and invoice tables.
    The augmentation is external data about the entities in your data in the form of _taxonomies_
    and _ontologies_ in layers.
  * This is inherently a graph-theoretic undertaking and traditional databases are not suitable.
    Usually, a dedicated graph processing engine is used, separate from the main database.
  * With Cozo, it is possible to keep your live data and knowledge graph analysis together,
    and importing new external data and doing analysis is just a few lines of code away.
    This ease of use means that you will do the analysis much more often, with a perhaps much wider scope.

## Status of the project

Cozo is very young and **not** production-ready yet, 
but we encourage you to try it out for your use case.
Any feedback is welcome.

Versions before 1.0 do not promise syntax/API stability or storage compatibility.
We promise that when you try to open database files created with an incompatible version,
Cozo will at least refuse to start instead of silently corrupting your data.

## Plans for development

In the near term, before we reach version 1.0:

* Backup/restore functionality
* Many, many more tests to ensure correctness
* Benchmarks

Further down the road:

* More tuning options
* Streaming/reactive data
* Extension system
  * The core of Cozo should be kept small at all times. Additional functionalities should be in extensions for the user to choose from. 
  * What can be extended: datatypes, functions, aggregations, and fixed algorithms.
  * Extensions should be written in a compiled language such as Rust or C++ and compiled into a dynamic library, to be loaded by Cozo at runtime.
  * There will probably be a few "official" extension bundles, such as
    * arbitrary precision arithmetic
    * full-text "indexing" and searching
    * relations that can emulate spatial and other types of non-lexicographic indices
    * reading from external databases directly
    * more exotic graph algorithms

Ideas and discussions are welcome.

## Cozo's storage engine

Cozo is written in Rust, with [RocksDB](http://rocksdb.org/) as the storage engine.
We manually wrote the C++/Rust bindings for RocksDB with [cxx](https://cxx.rs/). 

## Contributing

General discussions should go [here](https://github.com/cozodb/cozo/discussions). 
We also have a [Discord channel](https://discord.gg/GFpjQ9m866).

If you find a bug, first search for [past issues](https://github.com/cozodb/cozo/issues) to see
if it has already been reported. If not, open a new issue.

Contributions to code or other materials should be done via [pull requests](https://github.com/cozodb/cozo/pulls).
You will be guided to sign a CLA the first time you contribute.

## Licensing

The contents of this project are licensed under AGPL-3.0 or later, with the following exceptions:

* Contents in the `cozorocks` directory are licensed under MIT, or Apache-2.0, or BSD-3-Clause;
* Contents in the `docs` directory are licensed under CC BY-SA 4.0.