<img src="static/logo_c.png" width="200" height="175" alt="Logo">

[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/cozodb/cozo/Build)](https://github.com/cozodb/cozo/actions/workflows/build.yml)
[![Crates.io](https://img.shields.io/crates/v/cozo)](https://crates.io/crates/cozo)
[![GitHub](https://img.shields.io/github/license/cozodb/cozo)](https://github.com/cozodb/cozo/blob/main/LICENSE.txt)

# `cozo`

A general-purpose, transactional, relational database
that uses **Datalog** for query, **embeddable** but can also handle huge amount of data and concurrency, and focuses on **graph** data and algorithms.

### What do you mean by _embeddable_?

### What is so cool about _Datalog_?

### Why _graphs_?

## Learning

Usually, to learn a database, you need to install it first.
This is unnecessary for Cozo as a testimony to its extreme embeddability, since you can run 
a complete Cozo instance in your browser (at near native speed for most operations)!

So open up the [Cozo in WASM page](https://cozodb.github.io/wasm-demo/), and then:

* Follow the [tutorial](https://nbviewer.org/github/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb) to learn the basics;
* Read the [manual](https://cozodb.github.io/current/manual/) for the finer points.

After you have decided that Cozo is worth experimenting for your next project, you can scroll down to learn 
how to use it embedded (or not) in your favourite environment.

### Teasers

If you just want a taste of what querying with Cozo is like, here it is.
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

| count_unique(to) |
|------------------|
| 3462             |

What are the two most difficult to reach airports
by the minimum number of hops required,
starting from `FRA`?

```
reachable[to] := *route{fr: 'FRA', to}
reachable[to] := reachable[stop], *route{fr: stop, to}
?[count_unique(to)] := reachable[to]
```

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

<pre><span style="color: rgb(204, 0, 0) !important;">eval::unbound_symb_in_head</span><span>

  </span><span style="color: rgb(204, 0, 0) !important;">×</span><span> Symbol 'Y' in rule head is unbound
   ╭────
 </span><span style="color: rgba(0, 0, 0, 0.5) !important;">1</span><span> │ ?[x, Y] := x = 1, y = x + 1
   · </span><span style="font-weight: bold !important; color: rgb(255, 0, 255) !important;">     ─</span><span>
   ╰────
</span><span style="color: rgb(0, 153, 255) !important;">  help: </span><span>Note that symbols occurring only in negated positions are not considered bound
</span></pre>

## Install

How you install Cozo depends on where you want to use it from.
Follow the links in the table below:

| Language/Environment                                          | Official platform support                                                                                        | Storage |
|---------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------|---------|
| [Python](https://github.com/cozodb/pycozo)                    | Linux (x86_64), Mac (ARM64, x86_64), Windows (x86_64)                                                                   | MQR     |
| [NodeJS](./cozo-lib-nodejs)                                   | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Web browser](./cozo-lib-wasm)                                | Modern browsers supporting [web assembly](https://developer.mozilla.org/en-US/docs/WebAssembly#browser_compatibility)   | M       |
| [Java (JVM)](https://github.com/cozodb/cozo-lib-java)         | Linux (x86_64), Mac (ARM64, x86_64), Windows (x86_64)                                                                   | MQR     |
| [Clojure (JVM)](https://github.com/cozodb/cozo-clj)           | Linux (x86_64), Mac (ARM64, x86_64), Windows (x86_64)                                                                   | MQR     |
| [Android](https://github.com/cozodb/cozo-lib-android)         | Android (ARM64, ARMv7, x86_64, x86)                                                                                     | MQ      |
| [iOS/MacOS (Swift)](https://github.com/cozodb/cozo-lib-swift) | iOS (ARM64, simulators), Mac (ARM64, x86_64)                                                                            | MQ      |
| [Rust](https://docs.rs/cozo/)                                 | Source only, usable on any [platform](https://doc.rust-lang.org/nightly/rustc/platform-support.html) with `std` support | MQRST   |
| [Golang](https://github.com/cozodb/cozo-lib-go)               | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [C/C++/language with C FFI](./cozo-lib-c)                     | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQR     |
| [Standalone HTTP server](./cozoserver)                        | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                                            | MQRST   |

For the storage column:

* M: in-memory, non-persistent backend
* Q: [SQLite](https://www.sqlite.org/) storage backend
* R: [RocksDB](http://rocksdb.org/) storage backend
* S: [Sled](https://github.com/spacejam/sled) storage backend
* T: [TiKV](https://tikv.org/) distributed storage backend

The [Rust doc](https://docs.rs/cozo/) has some tips on choosing storage,
which is helpful even if you are not using Rust. 
Even if a storage/platform is not officially supported, 
you can still try to compile your own version to use, maybe with some tweaks in code.

## Architecture

## Status of the project

Cozo is very young and **not** production-ready yet,
but we encourage you to try it out for your use case.
Any feedback is welcome.

Versions before 1.0 do not promise syntax/API stability or storage compatibility.

## Licensing

This project are licensed under MPL-2.0 or later.
See [here](CONTRIBUTING.md) if you are interested in contributing to the project.