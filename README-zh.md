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

# Cozo 数据库

## 简介

[ 中文文档 | [English](./README.md) ]

Cozo是一个通用事务性关系型数据库：

* 是一个**可嵌入**数据库；
* 使用**Datalog**作为查询语句；
* 专注于**图数据、图算法**；
* 可进行**时光穿梭**查询；
* 支持**高性能、高并发**。

### “可嵌入”是什么意思？

如果你能在不联网的手机上使用某个数据库，那它大概率就是嵌入式的。 SQLite是嵌入式数据库。MySQL、Postgres、Oracle是客户端—服务器（CS）架构的数据库。

> 如果一个数据库与你的主程序在同一进程中运行，那么它就是 _嵌入式_ 数据库。与此相对，在使用 _客户端—服务器_ 架构的数据库时，主程序通过数据库客户库连接到数据库服务器（可能运行在一个独立的机器上）。嵌入式数据库通常不需要额外设置，可以在更广泛的环境中使用。
>
> 因为Cozo同时也支持客户端—服务器模式运行，所以我们说它是 _可嵌入_ 数据库而不是仅仅是 _嵌入式_ 数据库。在客户端—服务器模式下，服务器资源可以得到更好的运用，并支持比嵌入式模式更多的并发性能。

### “图数据”有什么用？

数据在本质上是相互关联、自关联的，这种关联的数学表达便是 _图_ 。只有将这些关联性考虑进去，才能更深入地洞察数据背后的逻辑。

> 大多数现有的 _图数据库_ 强制要求按照属性图（property graph）的范式存储数据。与此相对，Cozo的存储范式是传统的关系数据模型。关系数据模型的实现具有存储简单、功能强劲等优点，并且处理图数据也毫无问题。对于数据的数据洞察常常需要挖掘隐含在数据中内关联，而关系数据模型作为关系 _代数_（relational algebra）可以很好地处理此类问题。比较而言，属性图模型处理此类问题较为吃力，因为其不构成一个代数，可组合性弱。

### “Datalog”好在哪儿？

Datalog可表达所有的 _关系型查询_。_递归_ 的表达是 Datalog 的强项，且通常比相应的SQL查询中运行得更快。Datalog的组合性、模块性都很优秀，你可以一层一层地清晰地表达你的查询。

> 递归对于图查询尤其重要。Cozo的Datalog方言CozoScript允许在含有（安全的）聚合查询规则中使用递归，进一步增强了Datalog的递归查询能力。同时，Cozo内置了图分析中常用的一些递归算法（如PageRank等）的高性能实现，可以简便的直接调用。
>
> 当你对Datalog有进一步了解以后，你就会发现Datalog的 _规则_ 就像编程语言中的函数。规则的特点就是其可组合性：将一个查询分解成多个渐进的规则可使它更加清晰、更易维护，且也不会有效率上的损失。与此相对，复杂的SQL查询语句通常表现为多层嵌套的“select-from-where”的形式，可读性不高。

### 时光穿梭？

在数据库中，“时光穿梭”意味着跟踪数据随时间的变化，并允许针对在特定时间的数据快照执行查询以获得数据的历史视图。

> 在某种意义上，这使得你的数据库变得 _不可变_，因为没有数据会被真正删除。
> 
> 在Cozo中，不是所有数据都自动支持时间旅行：某个表是否有这个能力需要你来决定。这是因为每一项额外的功能都有其代价，而如果你不使用这个功能，其代价你就不必承担。
> 
> 这里有一个关于时光穿梭的[小故事](https://docs.cozodb.org/en/latest/releases/v0.4.html)，可以帮助你了解其一些应用场景。


### “高性能、高并发”，有多高？

我们在一台2020年的Mac Mini上，使用RocksDB持久性存储引擎（Cozo支持许多存储引擎）做了一些性能测试：

* 对一个有160万行的表进行OLTP查询：混合读、写、改的事务性查询可达到每秒10万次，而对于只读查询，可达到每秒25万次。在此过程中，数据库使用的内存峰值约为50MB。
* 备份速度约为每秒100万行，恢复速度约为每秒40万行。备份、恢复的速度不管表本身有多大都差不多。
* OLAP查询：扫描一个有160万行的表大约需要1秒（取决于具体操作略有不同，上下2倍以内）。查询所需的时间大致与查询所涉及的行数成比例，内存的使用主要由返回集的大小决定。
* 对于一个有3100万条边的图数据表，“两跳”图查询（如查询某人的朋友的朋友都有谁）可在1毫秒内完成。
* Pagerank算法速度。1万个顶点和12万条边：50毫秒内完成；10个万顶点和170万条边：大约在1秒内完成；160万个顶点和32万条边：大约在30秒内完成。

更多的细节请看[此文章](https://docs.cozodb.org/en/latest/releases/v0.3.html)。

## 学习

一般来说，你得先安装数据库才能学习怎么使用它。但Cozo是“嵌入式”的，所以它可以直接在浏览器里通过WASM运行，省去了安装的麻烦，而大多数操作的速度也和原生的差不多。打开[WASM里面跑的Cozo页面](https://www.cozodb.org/wasm-demo/)，然后就可以开始学了：

* [Cozo辅导课](https://docs.cozodb.org/en/latest/tutorial.html)——学习基础知识

当然你也可以先翻到后面了解如何在你熟悉的环境里安装原生Cozo数据库，再通过以上资料学习。

### 一些示例

以下给出一些示例，可以在正式学习之前了解一下Cozo的查询长什么样。

假设我们有个表叫做`*route`，含有两列，名称叫做`fr`和`to`，存的都是机场的代码（比如`FRA`就是法兰克福机场的代码），而每行数据表示一个航线。

从`FRA`可以直接飞到多少个机场：
```
?[count_unique(to)] := *route{fr: 'FRA', to}
```

| count_unique(to) |
|------------------|
| 310              |


从`FRA`出发，经停一次，可以飞到多少个机场：
```
?[count_unique(to)] := *route{fr: 'FRA', to: 'stop},
                       *route{fr: stop, to}
```

| count_unique(to) |
|------------------|
| 2222             |

从`FRA`出发，经停任意次数，可以到达多少个机场：
```
reachable[to] := *route{fr: 'FRA', to}
reachable[to] := reachable[stop], *route{fr: stop, to}
?[count_unique(to)] := reachable[to]
```

| count_unique(to) |
|------------------|
| 3462             |

从`FRA`出发，按所需的最少经停次数计算，给出最难到达的两个机场：
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

按实际路程计算，给出`FRA`和`YPO`这两个机场之间最短的路径：
```
start[] <- [['FRA']]
end[] <- [['YPO]]
?[src, dst, distance, path] <~ ShortestPathDijkstra(*route[], start[], end[])
```

| src | dst | distance | path                                                   |
|-----|-----|----------|--------------------------------------------------------|
| FRA | YPO | 4544.0   | `["FRA","YUL","YVO","YKQ","YMO","YFA","ZKE","YAT","YPO"]` |

如果查询语句有错误，Cozo会尝试提供明确、有用的错误信息：
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

## 安装

建议先[试用Cozo](#学习)，再安装。当然反过来也可以。

如何安装Cozo取决于所使用的语言与环境，如下表：

| 语言/环境                                                 | 官方支持的平台                                                                                              | 存储引擎  |
|-------------------------------------------------------|------------------------------------------------------------------------------------------------------|-------|
| [Python](https://github.com/cozodb/pycozo)            | Linux (x86_64), Mac (ARM64, x86_64), Windows (x86_64)                                                | MQR   |
| [NodeJS](./cozo-lib-nodejs)                           | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [浏览器](./cozo-lib-wasm)                                | 支持[WASM](https://developer.mozilla.org/en-US/docs/WebAssembly#browser_compatibility)的浏览器（较新的浏览器全都支持） | M     |
| [Java (JVM)](https://github.com/cozodb/cozo-lib-java) | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [Clojure (JVM)](https://github.com/cozodb/cozo-clj)   | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [安卓](https://github.com/cozodb/cozo-lib-android)      | Android (ARM64, ARMv7, x86_64, x86)                                                                  | MQ    |
| [iOS/macOS (Swift)](./cozo-lib-swift)                 | iOS (ARM64, 模拟器), Mac (ARM64, x86_64)                                                                | MQ    |
| [Rust](https://docs.rs/cozo/)                         | 任何支持`std`的[平台](https://doc.rust-lang.org/nightly/rustc/platform-support.html)（源代码编译）                 | MQRST |
| [Go](https://github.com/cozodb/cozo-lib-go)           | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [C/C++/支持C FFI的语言](./cozo-lib-c)                      | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [独立的HTTP服务](./cozoserver)                             | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQRST |

“存储引擎”列中各个字母的含义：

* M: 基于内存的非持久性存储引擎
* Q: 基于[SQLite](https://www.sqlite.org/)的存储引擎
* R: 基于[RocksDB](http://rocksdb.org/)的存储引擎
* S: 基于[Sled](https://github.com/spacejam/sled)的存储引擎
* T: 基于[TiKV](https://tikv.org/)的分布式存储引擎

在Cozo的[Rust文档](https://docs.rs/cozo/)里有一些额外的选择存储的建议。

即使你的语言、平台、存储引擎不被官方支持，你也可以尝试自己编译（也许需要在代码中做一些调整）。

### 为Cozo优化RocksDB存储引擎

RocksDB本身就有非常多的选项，调整这些选项可以在特定的工作负载下达到更好的性能。当然Cozo“开箱”的设置就已经相当快了，所以对95%的用户来说，优化引擎本身是不必要的。

如果你是剩下的那5%：当你用RocksDB引擎创建CozoDB实例时，你需要提供一个存储数据的目录的路径（如果不存在将被创建）。你可以在这个目录里创建一个名为`options`的文件，这时RocksDB引擎会将其解读为[RocksDB选项文件](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File)
并应用其中的设置。如果你使用的是独立的`cozoserver`程序，此功能被激活时会有一条日志信息提示。

设置文件的内容相当繁杂，乱设置可能会造成数据库的各种问题。每次运行RocksDB引擎的数据库时，目录下的`data/OPTIONS-XXXXXX`文件会记录当前的设置，你可以将这些文件作为优化设置的基础。如果你不是RocksDB方面的专家，建议只改动那些你至少大概知道什么意思的数字型选项。

## 架构

Cozo数据库由三个垒起来的组成部分组成，其中每部分只调用下面那部分的接口。

<table>
<tbody>
<tr><td>(<i>用户代码</i>)</td></tr>
<tr><td>语言/环境包装</td></tr>
<tr><td>查询引擎</td></tr>
<tr><td>存储引擎</td></tr>
<tr><td>(<i>操作系统</i>)</td></tr>
</tbody>
</table>

### 存储引擎

存储引擎定义了一个存储接口（Rust中的`trait`），需要能够支持二进制数据的键值存储及范围扫描。目前官方的具体实现如下：

* 基于内存的非持久性存储引擎
* 基于[SQLite](https://www.sqlite.org/)的存储引擎
* 基于[RocksDB](http://rocksdb.org/)的存储引擎
* 基于[Sled](https://github.com/spacejam/sled)的存储引擎
* 基于[TiKV](https://tikv.org/)的分布式存储引擎

编译好的版本并不包含所有的引擎。这里面SQLite引擎有特殊地位：它也同时也是Cozo的备份文件格式，可以用来在不同引擎的Cozo数据库之间交换数据。Rust使用者也可以自己实现别的引擎。

所有的存储引擎都使用相同的 _面向行的_ 二进制数据存储格式。实现具体的存储引擎并不需要了解这种格式。这种格式在存储键是使用的是一种叫做[memcomparable](https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format)的格式，其好处为能够将数据行存储为一个字节数组，直接对这些字节数组按照字节的顺序顺序排序就会得到正确的语义排序。当然，这也意味着SQLite引擎中存储的数据直接用SQL查询得到的结果看起来是乱码。

### 查询引擎

查询引擎部分实现了以下功能：

* 函数、聚合算子、算法的定义
* 数据结构定义（schema）
* 数据库事务（transaction）
* 查询语句的编译
* 查询的执行

Cozo中大部分代码都是在实现这些功能。CozoScript手册中[有一章](https://docs.cozodb.org/en/latest/execution.html)简要介绍了查询执行的一些细节。

用户通过[Rust API](https://docs.rs/cozo/)来驱动查询引擎。

### 语言、环境封装

除Rust之外的所有语言、环境，都只是Rust API的进一步封装，使其在相应的环境中更容易使用。例如，在独立服务器（cozoserver）中，Rust的API被封装为HTTP端点，而在NodeJS中，同步的Rust API被封装为基于JavaScript运行时的异步调用。

你也可以尝试自己封装Rust API，使其可以用于其他语言。如果没有现成的目标语言与Rust之前的交互库，你可以考虑包装Cozo提供的基于C语言的API。在官方支持的语言中，只有Go直接封装了C语言的API。

## 项目进度

Cozo是一个非常年轻的项目。欢迎任何反馈。

1.0之前的版本不承诺语法、API的稳定性或存储兼容性。

## 许可证和贡献

本项目以MPL-2.0或更高版本授权。如果你有兴趣为该项目做贡献，请看[这里](CONTRIBUTING.md)。