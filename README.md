[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/cozodb/cozo/Build)](https://github.com/cozodb/cozo/actions/workflows/build.yml)
[![Crates.io](https://img.shields.io/crates/v/cozo)](https://crates.io/crates/cozo)
[![GitHub](https://img.shields.io/github/license/cozodb/cozo)](https://github.com/cozodb/cozo/blob/main/LICENSE.txt)

# `cozo`

A general-purpose, transactional, relational database
that uses Datalog for query, is embeddable, and focuses on graph data and algorithms.

## Features

* Relational database with [Datalog](https://en.wikipedia.org/wiki/Datalog) as the query language
    * Recursive queries, recursion through (safe) aggregations, capable of expressing complex graph operations and
      algorithms
    * Fixed rules for efficient whole-graph algorithms which integrate seamlessly with Datalog
    * Rich set of built-in functions and aggregations
* Easy to use from any programming language, or as a standalone program
    * [Embeddable](https://cozodb.github.io/current/manual/setup.html#embedding-cozo), with ready-to-use bindings for
      Python, NodeJS and Java
    * Single executable, trivial to deploy and run
    * [Jupyter](https://jupyter.org/) notebooks integration, plays well with the DataScience ecosystem
* Modern, clean, flexible syntax, informative error messages

## Teasers

Here `*route` is a relation with two columns `src` and `dst`,
representing a route between those airports.

Find airports reachable by one stop from Frankfurt Airport (code `FRA`):

TODO replace with images

```js
?[dst] := *route{src: 'FRA', dst: stop}, 
          *route{src: stop, dst}
```

Find airports reachable from Frankfurt with any number of stops
with code starting with the letter `A`:

```js
reachable[dst] := *route{src: 'FRA', dst}
reachable[dst] := reachable[src], *route{src, dst}
?[airport] := reachable[airport], starts_with(airport, 'A')
```

Compute the shortest path between Frankfurt and all airports in the world:

```js
shortest_paths[dst, shortest(path)] := *route{src: 'FRA', dst},
                                       path = ['FRA', dst]
shortest_paths[dst, shortest(path)] := shortest_paths[stop, prev_path], 
                                       *route{src: stop, dst},
                                       path = append(prev_path, dst)
?[dst, path] := shortest_paths[dst, path]
```

Compute the shortest path again, but with built-in algorithm:

```js
starting[airport] := airport = 'FRA'
?[src, dst, cost, path] <~ ShortestPathDijkstra(*route[], starting[])
```

Nice error messages when things go wrong:

xxx

## Getting started

First we need to get Cozo installed on the local machine. As Cozo is embeddable,
there are lots of options for how we run Cozo. Follow one of the following guides that suits you best:

* Embedded in Python with JupyterLab (best interactive experience, recommended if you are comfortable with the Python
  ecosystem)
* Embedded in Python
* Embedded in NodeJS (Javascript)
* Embedded in Java (or any JVM language)
* Embedded in Rust
* Client/server with HTTP API
* Embedded in C/C++, or any language that has a C FFI 
  (Golang, R, Haskell, CommonLisp, Julia, Fortran, C#, Swift, ...)

After you have it installed, you can start learning CozoScript:

* Start with the [Tutorial](https://nbviewer.org/github/cozodb/cozo/blob/main/docs/tutorial/tutorial.ipynb) to learn the
  basics;
* Continue with the [Manual](https://cozodb.github.io/current/manual/) for the fine points.

## Bug reports, discussions

If you encounter a bug, first search for [past issues](https://github.com/cozodb/cozo/issues) to see
if it has already been reported. If not, open a new issue.
Please provide sufficient information so that we can diagnose the problem faster.

Other discussions about Cozo should be in [GitHub discussions](https://github.com/cozodb/cozo/discussions).

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
    * With Cozo, on the other hand, [Horn clauses](https://en.wikipedia.org/wiki/Horn_clause)
      make it easy to break
      the logic into smaller pieces and write clear, easily testable queries.
      Furthermore, the deterministic evaluation order makes identifying and solving
      performance problems easier.
* Your data may be simple, even a single table, but it is inherently a graph.
    * We have seen an example in
      the [Tutorial](https://nbviewer.org/github/cozodb/cozo/blob/main/docs/tutorial/tutorial.ipynb):
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
    * The core of Cozo should be kept small at all times. Additional functionalities should be in extensions for the
      user to choose from.
    * What can be extended: datatypes, functions, aggregations, and fixed algorithms.
    * Extensions should be written in a compiled language such as Rust or C++ and compiled into a dynamic library, to be
      loaded by Cozo at runtime.
    * There will probably be a few "official" extension bundles, such as
        * arbitrary precision arithmetic
        * full-text "indexing" and searching
        * relations that can emulate spatial and other types of non-lexicographic indices
        * reading from external databases directly
        * more exotic graph algorithms

Ideas and discussions are welcome.

## Storage engine

Cozo is written in Rust, with [RocksDB](http://rocksdb.org/) as the storage engine
(this may change in the future).
We manually wrote the C++/Rust bindings for RocksDB with [cxx](https://cxx.rs/).

## Licensing

The contents of this project are licensed under AGPL-3.0 or later, except:

* Files under `cozorocks/`, `python/`, `nodejs/`, `java/` and `c/` are licensed under MIT, or Apache-2.0, or BSD-3-Clause;
* Files under `docs/` are licensed under CC BY-SA 4.0.