<img src="static/logo_c.png" width="200" height="175" alt="Logo">

[![tutorial](https://img.shields.io/badge/tutorial-latest-brightgreen)](https://github.com/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
[![manual](https://img.shields.io/badge/manual-latest-brightgreen)](https://cozodb.github.io/current/manual/)
[![cozo-node](https://img.shields.io/npm/v/cozo-node)](https://www.npmjs.com/package/cozo-node)
[![npm (web)](https://img.shields.io/npm/v/cozo-lib-wasm?label=browser)](https://www.npmjs.com/package/cozo-lib-wasm)
[![Crates.io](https://img.shields.io/crates/v/cozo)](https://crates.io/crates/cozo)
[![docs.rs](https://img.shields.io/docsrs/cozo?label=docs.rs)](https://docs.rs/cozo)
[![pypi](https://img.shields.io/pypi/v/pycozo)](https://pypi.org/project/pycozo/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/cozodb/cozo/Build)](https://github.com/cozodb/cozo/actions/workflows/build.yml)
[![GitHub](https://img.shields.io/github/license/cozodb/cozo)](https://github.com/cozodb/cozo/blob/main/LICENSE.txt)

# `cozo`

### Table of contents

1. [Introduction](#Introduction)
2. [Getting started](#Getting-started)
3. [Install](#Install)
4. [Architecture](#Architecture)
5. [Status of the project](#Status-of-the-project)
6. [Licensing and contributing](#Licensing-and-contributing)

## Introduction

Cozo is a general-purpose, transactional, relational database
that uses **Datalog** for query, is **embeddable** but can also handle huge amounts of data and concurrency, and focuses on **graph** data and algorithms.

### What does _embeddable_ mean here?

A database is almost surely embedded
if you can use it on a phone which _never_ connects to any network
(this situation is not as unusual as you might think). SQLite is embedded. MySQL/Postgres/Oracle are client-server.

> A database is _embedded_ if it runs in the same process as your main program.
This is in contradistinction to _client-server_ databases, where your program connects to
a database server (maybe running on a separate machine) via a client library. Embedded databases
generally require no setup and can be used in a much wider range of environments.
>
> We say Cozo is _embeddable_ instead of _embedded_ since you can also use it in client-server
mode, which can make better use of server resources and allow much more concurrency than
in embedded mode.

### Why _graphs_?

Because data are inherently interconnected. Most insights about data can only be obtained if
you take this interconnectedness into account.

> Most existing _graph_ databases start by requiring you to shoehorn your data into the labelled-property graph model.
We don't go this route because we think the traditional relational model is much easier to work with for
storing data, much more versatile, and can deal with graph data just fine. Even more importantly,
the most piercing insights about data usually come from graph structures _implicit_ several levels deep
in your data. The relational model, being an _algebra_, can deal with it just fine. The property graph model,
not so much, since that model is not very composable.

### What is so cool about _Datalog_?

Datalog can express all relational queries. _Recursion_ in Datalog is much easier to express,
much more powerful, and usually runs faster than in SQL. Datalog is also extremely composable:
you can build your queries piece by piece.

> Recursion is especially important for graph queries. Cozo's dialect of Datalog
> supercharges it even further by allowing recursion through a safe subset of aggregations,
> and by providing extremely efficient canned algorithms (such as PageRank) for the kinds of recursions
> frequently required in graph analysis.
>
> As you learn Datalog, you will discover that the _rules_ of Datalog are like functions
> in a programming language. Rules are composable, and decomposing a query into rules
> can make it clearer and more maintainable, with no loss in efficiency.
> This is unlike the monolithic approach taken by the SQL `select-from-where` in nested forms,
> which can sometimes read like [golfing](https://en.wikipedia.org/wiki/Code_golf).

## Getting started

Usually, to learn a database, you need to install it first.
This is unnecessary for Cozo as a testimony to its extreme embeddability, since you can run
a complete Cozo instance in your browser, at near-native speed for most operations!

So open up the [Cozo in WASM page](https://cozodb.github.io/wasm-demo/), and then:

* Follow the [tutorial](https://github.com/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb) to learn the basics;
* read the [manual](https://cozodb.github.io/current/manual/) for the finer points.

After you have decided that Cozo is worth experimenting with for your next project, you can scroll down to learn
how to use it embedded (or not) in your favourite environment.

### Teasers

If you are in a hurry and just want a taste of what querying with Cozo is like, here it is.
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
?[count_unique(to)] := *route{fr: 'FRA', to: 'stop},
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

| to  | path                                              | p_len |
|-----|---------------------------------------------------|-------|
| YPO | `["FRA","YYZ","YTS","YMO","YFA","ZKE","YAT","YPO"]` | 8     |
| BVI | `["FRA","AUH","BNE","ISA","BQL","BEU","BVI"]`        | 7     |

What is the shortest path between `FRA` and `YPO`, by actual distance travelled?

```
start[] <- [['FRA']]
end[] <- [['YPO]]
?[src, dst, distance, path] <~ ShortestPathDijkstra(*route[], start[], end[])
```

| src | dst | distance | path                                                   |
|-----|-----|----------|--------------------------------------------------------|
| FRA | YPO | 4544.0   | `["FRA","YUL","YVO","YKQ","YMO","YFA","ZKE","YAT","YPO"]` |

Cozo attempts to provide nice error messages when you make mistakes:

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

We suggest that you [try out](#Getting-started) Cozo before you install it in your environment.

How you install Cozo depends on which environment you want to use it in.
Follow the links in the table below:

| Language/Environment                                  | Official platform support                                                                                               | Storage |
|-------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|---------|
| [Python](https://github.com/cozodb/pycozo)            | Linux (x86_64), Mac (ARM64, x86_64), Windows (x86_64)                                                                   | MQR     |
| [NodeJS](./cozo-lib-nodejs)                           | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Web browser](./cozo-lib-wasm)                        | Modern browsers supporting [web assembly](https://developer.mozilla.org/en-US/docs/WebAssembly#browser_compatibility)   | M       |
| [Java (JVM)](https://github.com/cozodb/cozo-lib-java) | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Clojure (JVM)](https://github.com/cozodb/cozo-clj)   | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                                 | MQR     |
| [Android](https://github.com/cozodb/cozo-lib-android) | Android (ARM64, ARMv7, x86_64, x86)                                                                                     | MQ      |
| [iOS/MacOS (Swift)](./cozo-lib-swift)                 | iOS (ARM64, simulators), Mac (ARM64, x86_64)                                                                            | MQ      |
| [Rust](https://docs.rs/cozo/)                         | Source only, usable on any [platform](https://doc.rust-lang.org/nightly/rustc/platform-support.html) with `std` support | MQRST   |
| [Golang](https://github.com/cozodb/cozo-lib-go)       | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [C/C++/language with C FFI](./cozo-lib-c)             | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Standalone HTTP server](./cozoserver)                | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQRST   |

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

## Architecture

The Cozo database consists of three layers stuck on top of each other,
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
in the usual way, and access must be through the decoding process in Cozo.

### Query engine

The query engine part provides various functionalities:

* function/aggregation/algorithm definitions
* database schema
* transaction
* query compilation
* query execution

This part is where most of
the code of Cozo is concerned. The CozoScript manual [has a chapter](https://cozodb.github.io/current/manual/execution.html)
about the execution process.

Users interact with the query engine with the [Rust API](https://docs.rs/cozo/).

### Language/environment wrapper

For all languages/environments except Rust, this part just translates the Rust API
into something that can be easily consumed by the targets. For Rust, there is no wrapper.
For example, in the case of the standalone server, the Rust API is translated
into HTTP endpoints, whereas in the case of NodeJS, the (synchronous) Rust API
is translated into a series of asynchronous calls from the JavaScript runtime.

If you want to make Cozo usable in other languages, this part is where your focus
should be. Any existing generic interop libraries between Rust and your target language
would make the job much easier. Otherwise, you can consider wrapping the C API,
as this is supported by most languages. For the languages officially supported,
only Golang wraps the C API directly.

## Status of the project

Cozo is very young and **not** production-ready yet,
but we encourage you to try it out for your use case.
Any feedback is welcome.

Versions before 1.0 do not promise syntax/API stability or storage compatibility.

## Licensing and contributing

This project is licensed under MPL-2.0 or later.
See [here](CONTRIBUTING.md) if you are interested in contributing to the project.