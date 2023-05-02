<img src="static/logo_c.png" width="200" height="175" alt="Logo">

[![docs](https://img.shields.io/readthedocs/cozo/latest)](https://docs.cozodb.org/)
[![cozo-node](https://img.shields.io/npm/v/cozo-node)](https://www.npmjs.com/package/cozo-node)
[![npm (web)](https://img.shields.io/npm/v/cozo-lib-wasm?label=browser)](https://www.npmjs.com/package/cozo-lib-wasm)
[![Crates.io](https://img.shields.io/crates/v/cozo)](https://crates.io/crates/cozo)
[![docs.rs](https://img.shields.io/docsrs/cozo?label=docs.rs)](https://docs.rs/cozo)
[![pypi](https://img.shields.io/pypi/v/pycozo)](https://pypi.org/project/pycozo/)
[![java](https://img.shields.io/maven-central/v/io.github.cozodb/cozo_java?label=java)](https://mvnrepository.com/artifact/io.github.cozodb/cozo_java)
[![clj](https://img.shields.io/maven-central/v/io.github.cozodb/cozo-clj?label=clj)](https://mvnrepository.com/artifact/io.github.cozodb/cozo-clj)
[![android](https://img.shields.io/maven-central/v/io.github.cozodb/cozo_android?label=android)](https://mvnrepository.com/artifact/io.github.cozodb/cozo_android)
[![pod](https://img.shields.io/cocoapods/v/CozoSwiftBridge)](https://github.com/cozodb/cozo/tree/main/cozo-lib-swift)
[![Go](https://img.shields.io/github/v/release/cozodb/cozo-lib-go?label=go)](https://github.com/cozodb/cozo-lib-go)
[![C](https://img.shields.io/github/v/release/cozodb/cozo?label=C)](https://github.com/cozodb/cozo/releases)
[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/cozodb/cozo/build.yml?branch=main)](https://github.com/cozodb/cozo/actions/workflows/build.yml)
[![GitHub](https://img.shields.io/github/license/cozodb/cozo)](https://github.com/cozodb/cozo/blob/main/LICENSE.txt)

# `CozoDB`

### Table of contents

1. [Introduction](#Introduction)
2. [Getting started](#Getting-started)
3. [Install](#Install)
4. [Architecture](#Architecture)
5. [Status of the project](#Status-of-the-project)
6. [Licensing and contributing](#Licensing-and-contributing)

## 🎉🎉🎉 New versions 🎉🎉🎉

Version v0.7: after HNSW vector search from 0.6, in 0.7 we bring to you MinHash-LSH for near-duplicate search, full-text
search, Json value support and more! See [here](https://docs.cozodb.org/en/latest/releases/v0.7.html) for more details.

---

Version v0.6 released! This version brings vector search with HNSW indices inside Datalog, which can be integrated
seamlessly with powerful features like ad-hoc joins, recursive Datalog and classical whole-graph algorithms. This
significantly expanded the horizon of possibilities of CozoDB.

Highlights:

* You can now create HNSW (hierarchical navigable small world) indices on relations containing vectors.
* You can create multiple HNSW indices for the same relation by specifying filters dictating which rows should be
  indexed, or which vector(s) should be indexed for each row if the row contains multiple vectors.
* The vector search functionality is integrated within Datalog, meaning that you can use vectors (either explicitly
  given or coming from another relation) as pivots to perform unification into the indexed relations (roughly equivalent
  to table joins in SQL).
* Unification with vector search is semantically no different from regular unification, meaning that you can even use
  vector search in recursive Datalog, enabling extremely complex query logic.
* The HNSW index is no more than a hierarchy of proximity graphs. As an open, competent graph database, CozoDB exposes
  these graphs to the end user to be used as regular graphs in your query, so that all the usual techniques for dealing
  with them can now be applied, especially: community detection and other classical whole-graph algorithms.
* As with all mutations in CozoDB, the index is protected from corruption in the face of concurrent writes by using
  Multi-Version Concurrency Control (MVCC), and you can use multi-statement transactions for complex workflows.
* The index resides on disk as a regular relation (unless you use the purely in-memory storage option, of course).
  During querying, close to the absolute minimum amount of memory is used, and memory is freed as soon as the processing
  is done (thanks to Rust's RAII), so it can run on memory-constrained systems.
* The HNSW functionality is available for CozoDB on all platforms: in the server as a standalone service, in your
  Python, NodeJS, or Clojure programs om embedded or client mode, on your phone in embedded mode, even in the browser
  with the WASM backend.
* HNSW vector search in CozoDB is performant: we have optimized the index to the point where basic vector operations
  themselves have become a limiting factor (along with memcpy), and we are constantly finding ways to improve our new
  implementation of the HNSW algorithm further.

See [here](https://docs.cozodb.org/en/latest/releases/v0.6.html) for more details.

## Introduction

CozoDB is a general-purpose, transactional, relational database
that uses **Datalog** for query, is **embeddable** but can also handle huge amounts of data and concurrency,
and focuses on **graph** data and algorithms.
It supports **time travel** and it is **performant**!

### What does _embeddable_ mean here?

A database is almost surely embedded
if you can use it on a phone which _never_ connects to any network
(this situation is not as unusual as you might think). SQLite is embedded. MySQL/Postgres/Oracle are client-server.

> A database is _embedded_ if it runs in the same process as your main program.
> This is in contradistinction to _client-server_ databases, where your program connects to
> a database server (maybe running on a separate machine) via a client library. Embedded databases
> generally require no setup and can be used in a much wider range of environments.
>
> We say CozoDB is _embeddable_ instead of _embedded_ since you can also use it in client-server
> mode, which can make better use of server resources and allow much more concurrency than
> in embedded mode.

### Why _graphs_?

Because data are inherently interconnected. Most insights about data can only be obtained if
you take this interconnectedness into account.

> Most existing _graph_ databases start by requiring you to shoehorn your data into the labelled-property graph model.
> We don't go this route because we think the traditional relational model is much easier to work with for
> storing data, much more versatile, and can deal with graph data just fine. Even more importantly,
> the most piercing insights about data usually come from graph structures _implicit_ several levels deep
> in your data. The relational model, being an _algebra_, can deal with it just fine. The property graph model,
> not so much, since that model is not very composable.

### What is so cool about _Datalog_?

Datalog can express all _relational_ queries. _Recursion_ in Datalog is much easier to express,
much more powerful, and usually runs faster than in SQL. Datalog is also extremely composable:
you can build your queries piece by piece.

> Recursion is especially important for graph queries. CozoDB's dialect of Datalog
> supercharges it even further by allowing recursion through a safe subset of aggregations,
> and by providing extremely efficient canned algorithms (such as PageRank) for the kinds of recursions
> frequently required in graph analysis.
>
> As you learn Datalog, you will discover that the _rules_ of Datalog are like functions
> in a programming language. Rules are composable, and decomposing a query into rules
> can make it clearer and more maintainable, with no loss in efficiency.
> This is unlike the monolithic approach taken by the SQL `select-from-where` in nested forms,
> which can sometimes read like [golfing](https://en.wikipedia.org/wiki/Code_golf).

### Time travel?

Time travel in the database setting means
tracking changes to data over time
and allowing queries to be logically executed at a point in time
to get a historical view of the data.

> In a sense, this makes your database _immutable_,
> since nothing is really deleted from the database ever.
>
> In Cozo, instead of having all data automatically support
> time travel, we let you decide if you want the capability
> for each of your relation. Every extra functionality comes
> with its cost, and you don't want to pay the price if you don't use it.
>
> For the reason why you might want time travel for your data,
> we have written a [short story](https://docs.cozodb.org/en/latest/releases/v0.4.html).

### How performant?

On a 2020 Mac Mini with the RocksDB persistent storage engine (CozoDB supports many storage engines):

* Running OLTP queries for a relation with 1.6M rows, you can expect around 100K QPS (queries per second) for mixed
  read/write/update transactional queries, and more than 250K QPS for read-only queries, with database peak memory usage
  around 50MB.
* Speed for backup is around 1M rows per second, for restore is around 400K rows per second, and is insensitive to
  relation (table) size.
* For OLAP queries, it takes around 1 second (within a factor of 2, depending on the exact operations) to scan a table
  with 1.6M rows. The time a query takes scales roughly with the number of rows the query touches, with memory usage
  determined mainly by the size of the return set.
* Two-hop graph traversal completes in less than 1ms for a graph with 1.6M vertices and 31M edges.
* The Pagerank algorithm completes in around 50ms for a graph with 10K vertices and 120K edges, around 1 second for a
  graph with 100K vertices and 1.7M edges, and around 30 seconds for a graph with 1.6M vertices and 32M edges.

For more numbers and further details, we have a writeup
about performance [here](https://docs.cozodb.org/en/latest/releases/v0.3.html).

## Getting started

Usually, to learn a database, you need to install it first.
This is unnecessary for CozoDB as a testimony to its extreme embeddability, since you can run
a complete CozoDB instance in your browser, at near-native speed for most operations!

So open up the [CozoDB in WASM page](https://www.cozodb.org/wasm-demo/), and then:

* Follow the [tutorial](https://docs.cozodb.org/en/latest/tutorial.html).

Or you can skip ahead for the information about installing CozoDB into your favourite environment first.

### Teasers

If you are in a hurry and just want a taste of what querying with CozoDB is like, here it is.
In the following `*route` is a relation with two columns `fr` and `to`,
representing a route between those airports,
and `FRA` is the code for Frankfurt Airport.

How many airports are directly connected to `FRA`?

```
?[count_unique(to)] := *route{fr: 'FRA', to}
```

| count_unique(to) |
|------------------|
| 310              |

How many airports are reachable from `FRA` by one stop?

```
?[count_unique(to)] := *route{fr: 'FRA', to: stop},
                       *route{fr: stop, to}
```

| count_unique(to) |
|------------------|
| 2222             |

How many airports are reachable from `FRA` by any number of stops?

```
reachable[to] := *route{fr: 'FRA', to}
reachable[to] := reachable[stop], *route{fr: stop, to}
?[count_unique(to)] := reachable[to]
```

| count_unique(to) |
|------------------|
| 3462             |

What are the two most difficult-to-reach airports
by the minimum number of hops required,
starting from `FRA`?

```
shortest_paths[to, shortest(path)] := *route{fr: 'FRA', to},
                                      path = ['FRA', to]
shortest_paths[to, shortest(path)] := shortest_paths[stop, prev_path],
                                      *route{fr: stop, to},
                                      path = append(prev_path, to)
?[to, path, p_len] := shortest_paths[to, path], p_len = length(path)

:order -p_len
:limit 2
```

| to  | path                                                | p_len |
|-----|-----------------------------------------------------|-------|
| YPO | `["FRA","YYZ","YTS","YMO","YFA","ZKE","YAT","YPO"]` | 8     |
| BVI | `["FRA","AUH","BNE","ISA","BQL","BEU","BVI"]`       | 7     |

What is the shortest path between `FRA` and `YPO`, by actual distance travelled?

```
start[] <- [['FRA']]
end[] <- [['YPO]]
?[src, dst, distance, path] <~ ShortestPathDijkstra(*route[], start[], end[])
```

| src | dst | distance | path                                                      |
|-----|-----|----------|-----------------------------------------------------------|
| FRA | YPO | 4544.0   | `["FRA","YUL","YVO","YKQ","YMO","YFA","ZKE","YAT","YPO"]` |

CozoDB attempts to provide nice error messages when you make mistakes:

```
?[x, Y] := x = 1, y = x + 1
```

<pre><span style="color: rgb(204, 0, 0);">eval::unbound_symb_in_head</span><span>

  </span><span style="color: rgb(204, 0, 0);">×</span><span> Symbol 'Y' in rule head is unbound
   ╭────
 </span><span style="color: rgba(0, 0, 0, 0.5);">1</span><span> │ ?[x, Y] := x = 1, y = x + 1
   · </span><span style="font-weight: bold; color: rgb(255, 0, 255);">     ─</span><span>
   ╰────
</span><span style="color: rgb(0, 153, 255);">  help: </span><span>Note that symbols occurring only in negated positions are not considered bound
</span></pre>

## Install

We suggest that you [try out](#Getting-started) CozoDB before you install it in your environment.

How you install CozoDB depends on which environment you want to use it in.
Follow the links in the table below:

| Language/Environment                                  | Official platform support                                                                                               | Storage |
|-------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|---------|
| [Python](https://github.com/cozodb/pycozo)            | Linux (x86_64), Mac (ARM64, x86_64), Windows (x86_64)                                                                   | MQR     |
| [NodeJS](./cozo-lib-nodejs)                           | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Web browser](./cozo-lib-wasm)                        | Modern browsers supporting [web assembly](https://developer.mozilla.org/en-US/docs/WebAssembly#browser_compatibility)   | M       |
| [Java (JVM)](https://github.com/cozodb/cozo-lib-java) | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Clojure (JVM)](https://github.com/cozodb/cozo-clj)   | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Android](https://github.com/cozodb/cozo-lib-android) | Android (ARM64, ARMv7, x86_64, x86)                                                                                     | MQ      |
| [iOS/MacOS (Swift)](./cozo-lib-swift)                 | iOS (ARM64, simulators), Mac (ARM64, x86_64)                                                                            | MQ      |
| [Rust](https://docs.rs/cozo/)                         | Source only, usable on any [platform](https://doc.rust-lang.org/nightly/rustc/platform-support.html) with `std` support | MQRST   |
| [Golang](https://github.com/cozodb/cozo-lib-go)       | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [C/C++/language with C FFI](./cozo-lib-c)             | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Standalone HTTP server](./cozo-bin)                  | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQRST   |

For the storage column:

* M: in-memory, non-persistent backend
* Q: [SQLite](https://www.sqlite.org/) storage backend
* R: [RocksDB](http://rocksdb.org/) storage backend
* S: [Sled](https://github.com/spacejam/sled) storage backend
* T: [TiKV](https://tikv.org/) distributed storage backend

The [Rust doc](https://docs.rs/cozo/) has some tips on choosing storage,
which is helpful even if you are not using Rust.
Even if a storage/platform is not officially supported,
you can still try to compile your version to use, maybe with some tweaks in the code.

### Tuning the RocksDB backend for CozoDB

RocksDB has a lot of options, and by tuning them you can achieve better performance
for your workload. This is probably unnecessary for 95% of users, but if you are the
remaining 5%, CozoDB gives you the options to tune RocksDB directly if you are using the
RocksDB storage engine.

When you create the CozoDB instance with the RocksDB backend option, you are asked to
provide a path to a directory to store the data (will be created if it does not exist).
If you put a file named `options` inside this directory, the engine will expect this
to be a [RocksDB options file](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File)
and use it. If you are using the standalone `cozo` executable, you will get a log message if
this feature is activated.

Note that improperly set options can make your database misbehave!
In general, you should run your database once, copy the options file from `data/OPTIONS-XXXXXX`
from within your database directory, and use that as a base for your customization.
If you are not an expert on RocksDB, we suggest you limit your changes to adjusting those numerical
options that you at least have a vague understanding.

## Architecture

CozoDB consists of three layers stuck on top of each other,
with each layer only calling into the layer below:

<table>
<tbody>
<tr><td>(<i>User code</i>)</td></tr>
<tr><td>Language/environment wrapper</td></tr>
<tr><td>Query engine</td></tr>
<tr><td>Storage engine</td></tr>
<tr><td>(<i>Operating system</i>)</td></tr>
</tbody>
</table>

### Storage engine

The storage engine defines a storage `trait` for the storage backend, which is an interface
with required operations, mainly the provision of a key-value store for binary data
with range scan capabilities. There are various implementations:

* In-memory, non-persistent backend
* [SQLite](https://www.sqlite.org/) storage backend
* [RocksDB](http://rocksdb.org/) storage backend
* [Sled](https://github.com/spacejam/sled) storage backend
* [TiKV](https://tikv.org/) distributed storage backend

Depending on the build configuration, not all backends may be available
in a binary release.
The SQLite backend is special in that it is also used as the backup file format,
which allows the exchange of data between databases with different backends.
If you are using the database embedded in Rust, you can even provide your own
custom backend.

The storage engine also defines a _row-oriented_ binary data format, which the storage
engine implementation does not need to know anything about.
This format contains an implementation of the
[memcomparable format](https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format)
used for the keys, which enables the storage of rows of data as binary blobs
that, when sorted lexicographically, give the correct order.
This also means that data files for the SQLite backend cannot be queried with SQL
in the usual way, and access must be through the decoding process in CozoDB.

### Query engine

The query engine part provides various functionalities:

* function/aggregation/algorithm definitions
* database schema
* transaction
* query compilation
* query execution

This part is where most of
the code of CozoDB is concerned. The CozoScript manual [has a chapter](https://docs.cozodb.org/en/latest/execution.html)
about the execution process.

Users interact with the query engine with the [Rust API](https://docs.rs/cozo/).

### Language/environment wrapper

For all languages/environments except Rust, this part just translates the Rust API
into something that can be easily consumed by the targets. For Rust, there is no wrapper.
For example, in the case of the standalone server, the Rust API is translated
into HTTP endpoints, whereas in the case of NodeJS, the (synchronous) Rust API
is translated into a series of asynchronous calls from the JavaScript runtime.

If you want to make CozoDB usable in other languages, this part is where your focus
should be. Any existing generic interop libraries between Rust and your target language
would make the job much easier. Otherwise, you can consider wrapping the C API,
as this is supported by most languages. For the languages officially supported,
only Golang wraps the C API directly.

## Status of the project

CozoDB is still very young, but we encourage you to try it out for your use case.
Any feedback is welcome.

Versions before 1.0 do not promise syntax/API stability or storage compatibility.

## Licensing and contributing

This project is licensed under MPL-2.0 or later.
See [here](CONTRIBUTING.md) if you are interested in contributing to the project.
