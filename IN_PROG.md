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

In the following, `:route` refers to a relation with two columns named `src` and `dst`, 
representing routes between airports.

Find airports reachable by one stop from Frankfurt Airport (`FRA`), the busiest airport in the world:

```js
?[dst] := :route{src: 'FRA', dst: stop}, 
          :route{src: stop, dst}
```

Find _all_ airports reachable from Frankfurt (i.e., the transitive closure):

```js
reachable[dst] := :route{src: 'FRA', dst}
reachable[dst] := reachable[src], :route{src, dst}
?[airport] := reachable[airport]
```

Compute the shortest path between Frankfurt and all airports in the world with recursion through aggregation:

```js
shortest_paths[dst, shortest(path)] := :route{src: 'FRA', dst},
                                       path = ['FRA', dst]
shortest_paths[dst, shortest(path)] := shortest_paths[stop, prev_path], 
                                       :route{src: stop, dst},
                                       path = append(prev_path, dst)
?[dst, path] := shortest_paths[dst, path]
```

Use a fixed rule to compute the shortest path:

```js
starting[airport] := airport = 'FRA'
?[src, dst, cost, path] <~ ShortestPathDijkstra(:route[], starting[])
```

## Learning Cozo

* Start with the [Tutorial](https://cozodb.github.io/current/tutorial.html) to learn the basics.
* Continue with the [Manual](https://cozodb.github.io/current/manual/) to understand the fine points.


## Use cases

Even though Cozo is a general purpose database and 
in principle can replace established, well-tested solutions such as PostgreSQL and SQLite,
that's not our intention when we wrote Cozo, 
nor do we recommend it if the established solutions already solve all your problems well.
Instead, we have specific use cases that the traditional databases do not provide
a sufficient solution.

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
Outside the storage layer, Cozo is 100% safe rust.

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