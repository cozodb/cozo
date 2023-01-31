<img src="static/logo_c.png" width="200" height="175" alt="Logo">

[![docs](https://img.shields.io/readthedocs/cozo/latest)](https://docs.cozodb.org/zh_CN/latest/)
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

Cozo是一个事务型关系型数据库：

* 一个 **可嵌入** 的数据库；
* 一个使用 **Datalog** 作为查询语句的数据库；
* 一个专注于 **图数据、图算法** 的数据库；
* 一个可进行 **历史穿梭** 查询的数据库；
* 一个支持 **高性能、高并发** 的数据库。

### “可嵌入”是什么意思？

如果某个数据库能在不联网的手机上使用，那它大概就是嵌入式的。举例来说，SQLite 是嵌入式的，而 MySQL、Postgres、Oracle 等不是（它们是客户端—服务器（CS）架构的数据库）。

> 如果数据库与你的主程序在同一进程中运行，那么它就是 _嵌入式_ 数据库。与此相对，在使用 _客户端—服务器_ 架构的数据库时，主程序需要通过特定的接口（通常是网络接口）访问数据库，而数据库也可能运行在另一台机器或独立的集群上。嵌入式数据库使用简单，资源占用少，并可以在更广泛的环境中使用。
>
> Cozo 同时也支持以客户端—服务器模式运行。因此，Cozo 是一个 _可嵌入_ 而不是仅仅是 _嵌入式_ 的数据库。在客户端—服务器模式下，Cozo 可以更充分地发挥服务器的性能。

### “图数据”有什么用？

从本质上来说，数据一定是相互关联、自关联的，而这种关联的数学表达便是 _图_ （也叫 _网络_）。只有考虑这些关联，才能更深入地洞察数据背后的逻辑。

> 大多数现有的 _图数据库_ 强制要求按照属性图（property graph）的范式存储数据。与此相对，Cozo 使用传统的关系数据模型。关系数据模型有存储逻辑简单、功能强劲等优点，并且处理图数据也毫无问题。更重要的是，数据的洞察常常需要挖掘隐含的关联，而关系数据模型作为关系 _代数_（relational algebra）可以很好地处理此类问题。比较而言，因为其不构成一个代数，属性图模型仅仅能够将显性的图关系作为图数据处理，可组合性很弱。

### “Datalog”好在哪儿？

Datalog 1977 年便出现了，它可表达所有的 _关系型查询_，而它与 SQL 比起来的优势在于其对 _递归_ 的表达。由于执行逻辑不同，Datalog 对于递归的运行，通常比相应的 SQL 查询更快。Datalog 的可组合性、模块性都很优秀，使用它，你可以逐层、清晰地表达所需的查询。

> 递归对于图查询尤其重要。Cozo 使用的 Datalog 方言 叫做 CozoScript，其允许在一定条件下混合使用聚合查询与递归，从而进一步增强了 Datalog 的表达能力。同时，Cozo内置了图分析中常用的一些算法（如 PageRank 等），调用简单。
>
> 对 Datalog 有进一步了解以后，你会发现 Datalog 的 _规则_ 类似于编程语言中的函数。规则的一大特点是其可组合性：将一个查询分解为多个渐进的规则可使查询更清晰、易维护，且不会有效率上的损失。与此相对的，复杂的 SQL 查询语句通常表达为多层嵌套的“select-from-where”，可读性、可维护性都不高。

### 历史穿梭？

在数据库中，“历史穿梭”的意思是记录数据的一切变化，以允许针对某一时刻的数据进行执行查询，用来窥探历史。

> 在某种意义上，这使数据库成为 _不可变_ 数据库，因为没有数据会被真正删除。
> 
> 每一项额外的功能都有其代价。如果不使用某个功能，理想的状态是不必为这个功能的代价埋单。在 Cozo 中，不是所有数据表都自动支持历史穿梭，这就把是否需要此功能、是否愿意支付代价的选择权交到了用户手里。
> 
> [这个](https://docs.cozodb.org/zh_CN/latest/releases/v0.4.html)关于历史穿梭的小故事可能启发出一些历史穿梭的应用场景。


### “高性能、高并发”，有多高？

我们在一台 2020 年的 Mac Mini 上，使用 RocksDB 持久性存储引擎（Cozo 支持多种存储引擎）做了性能测试：

* 对一个有 160 万行的表进行查询：读、写、改的混合事务性查询可达到每秒 10 万次，而只读查询可达到每秒 25 万次。在此过程中，数据库使用的内存峰值仅为50MB。
* 备份数据的速度为每秒约 100 万行，恢复速度为每秒约 40 万行。备份、恢复的速度不随表单数据增长而变慢。
* 分析查询：扫描一个有 160 万行的表大约需要 1 秒（根据具体查询语句大约有上下 2 倍以内的差异）。查询所需时间与查询所涉及的行数大致成比例，而内存使用主要决定于返回集合的大小。
* 对于一个有 160 万个顶点，3100 万条边的图数据表，“两跳”图查询（如查询某人的朋友的朋友都有谁）可在 1 毫秒内完成。
* Pagerank 算法速度：1 万个顶点，12 万条边：50 毫秒以内；10 个万顶点，170 万条边：1 秒以内；160 万个顶点，3100 万条边：30秒以内。

更多的细节参见[此文](https://docs.cozodb.org/zh_CN/latest/releases/v0.3.html)。

## 学习 Cozo

你得先安装一个数据库才能开始学，对吧？不一定：Cozo 是“嵌入式”的，所以我们直接把它通过 WASM 嵌入到浏览器里了！打开[这个页面](https://www.cozodb.org/wasm-demo/)，然后：

* [Cozo 入门教程](https://docs.cozodb.org/zh_CN/latest/tutorial.html)

当然也可以一步到位：先翻到后面了解如何在熟悉的环境里安装原生 Cozo 数据库，再开始学习。

### 一些示例

通过以下示例，可在正式开始学习之前对 Cozo 的查询先有个感性认识。

假设有个表，名为 `*route`，含有两列，名为 `fr` 和 `to`，其中数据为机场代码（如 `FRA` 是法兰克福机场的代码），且每行数据表示一个飞行航线。

从 `FRA` 可以不转机到达多少个机场：
```
?[count_unique(to)] := *route{fr: 'FRA', to}
```

| count_unique(to) |
|------------------|
| 310              |

从 `FRA` 出发，转机一次，可以到达多少个机场：
```
?[count_unique(to)] := *route{fr: 'FRA', to: stop},
                       *route{fr: stop, to}
```

| count_unique(to) |
|------------------|
| 2222             |

从 `FRA` 出发，转机任意次，可以到达多少个机场：
```
reachable[to] := *route{fr: 'FRA', to}
reachable[to] := reachable[stop], *route{fr: stop, to}
?[count_unique(to)] := reachable[to]
```

| count_unique(to) |
|------------------|
| 3462             |

从 `FRA` 出发，按所需的最少转机次数排序，到达哪两个机场需要最多的转机次数：
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

`FRA` 和 `YPO` 这两个机场之间最短的路径以及其实际飞行里程是多少：
```
start[] <- [['FRA']]
end[] <- [['YPO]]
?[src, dst, distance, path] <~ ShortestPathDijkstra(*route[], start[], end[])
```

| src | dst | distance | path                                                   |
|-----|-----|----------|--------------------------------------------------------|
| FRA | YPO | 4544.0   | `["FRA","YUL","YVO","YKQ","YMO","YFA","ZKE","YAT","YPO"]` |

当查询语句有错时，Cozo 会提供明确有用的错误信息：
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

## 安装 Cozo

建议先学习，再安装。当然反过来我们也不反对。

Cozo 可以安装在一大堆不同的语言与环境中：

| 语言/环境                                                                                                 | 官方支持的平台                                                                                              | 存储引擎  |
|-------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------|-------|
| [Python](https://github.com/cozodb/pycozo)（[国内镜像](https://gitee.com/cozodb/pycozo)）                   | Linux (x86_64), Mac (ARM64, x86_64), Windows (x86_64)                                                | MQR   |
| [NodeJS](./cozo-lib-nodejs)                                                                           | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [浏览器](./cozo-lib-wasm)                                                                                | 支持[WASM](https://developer.mozilla.org/en-US/docs/WebAssembly#browser_compatibility)的浏览器（较新的浏览器全都支持） | M     |
| [Java (JVM)](https://github.com/cozodb/cozo-lib-java)（[国内镜像](https://gitee.com/cozodb/cozo-lib-java)） | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [Clojure (JVM)](https://github.com/cozodb/cozo-clj)（[国内镜像](https://gitee.com/cozodb/cozo-clj)）        | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [安卓](https://github.com/cozodb/cozo-lib-android)（[国内镜像](https://gitee.com/cozodb/cozo-lib-android)）   | Android (ARM64, ARMv7, x86_64, x86)                                                                  | MQ    |
| [iOS/macOS (Swift)](./cozo-lib-swift)                                                                 | iOS (ARM64, 模拟器), Mac (ARM64, x86_64)                                                                | MQ    |
| [Rust](https://docs.rs/cozo/)                                                                         | 任何支持`std`的[平台](https://doc.rust-lang.org/nightly/rustc/platform-support.html)（源代码编译）                 | MQRST |
| [Go](https://github.com/cozodb/cozo-lib-go)（[国内镜像](https://gitee.com/cozodb/cozo-lib-go)）             | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [C/C++/支持 C FFI 的语言](./cozo-lib-c)                                                                    | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQR   |
| [独立的 HTTP 服务](./cozo-bin)                                                                             | Linux (x86_64, ARM64), Mac (ARM64, x86_64), Windows (x86_64)                                         | MQRST |

“存储引擎”列中各个字母的含义：

* M: 基于内存的非持久性存储引擎
* Q: 基于 [SQLite](https://www.sqlite.org/) 的存储引擎
* R: 基于 [RocksDB](http://rocksdb.org/) 的存储引擎
* S: 基于 [Sled](https://github.com/spacejam/sled) 的存储引擎
* T: 基于 [TiKV](https://tikv.org/) 的分布式存储引擎

Cozo 的 [Rust API 文档](https://docs.rs/cozo/)（英文）中有一些额外的关于存储选择的建议。

你也可以尝试为其它平台、语言、引擎自行编译 Cozo。可能需要调整一些代码，但总体来说不难。

### 优化基于 RocksDB 的存储引擎

RocksDB 有五花八门的选项以供用户进行性能调优。但是调优这个问题太复杂了，就连 RocksDB 他们自己也搞不定，所以实际生产中他们用的是强化学习来自动调优。对于 95% 的用户来说，费这个劲根本不值得，尤其是 Cozo “开箱”的设置就已经相当快、足够快了。

如果你坚信你是剩下那 5% 里面的：当你用 RocksDB 引擎创建 CozoDB 实例时，你提供过一个存储数据的目录路径。如果在这个目录里创建一个名为`options`的文件，RocksDB 引擎便会将其解读为 [RocksDB 选项文件](https://github.com/facebook/rocksdb/wiki/RocksDB-Options-File)
并应用其中的设置。如果使用的是独立的 `cozo` 程序，激活此功能时会有一条提示日志。

每次 RocksDB 引擎启动时，存储目录下的 `data/OPTIONS-XXXXXX` 文件会记录当前应用设置。你可以把这个文件拷贝出来，在其基础上修改。如果你不是 RocksDB 的专家，建议只改动那些你大概知道什么意思的数字型选项。设置不当可能会搞乱、搞坏数据库。

## Cozo 的架构

Cozo 数据库有三个上下游部分组成，其中每部分只调用下游部分的接口。

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

在存储引擎这一部分里，Cozo 定义了一个存储接口（Rust 中的 `trait`），这个接口的功能是对二进制数据的键值进行存储及范围扫描。目前这个接口有以下官方实现：

* 基于内存的非持久性存储引擎
* 基于 [SQLite](https://www.sqlite.org/) 的存储引擎
* 基于 [RocksDB](http://rocksdb.org/) 的存储引擎
* 基于 [Sled](https://github.com/spacejam/sled) 的存储引擎
* 基于 [TiKV](https://tikv.org/) 的分布式存储引擎

不是所有的二进制包都包含以上所有引擎。这些引擎中，SQLite 引擎具有特殊地位：Cozo 使用它的文件作为备份文件，用以在不同引擎的 Cozo 之间交换数据。Rust 使用者可以轻松实现自己的引擎（不是说写一个引擎很轻松，这里意思是把现有的引擎接入到 Cozo 里很轻松）。

Cozo 使用 _面向行_ 而非 _面向列_ 的二进制存储格式。在这个格式中，对键的存储通过 [memcomparable](https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format) 的方法将复合键存储为一个字节数组，而直接对这些字节数组按照字节顺序排序就能得到正确的语义排序。这也意味着直接用 SQL 查询在 SQLite 引擎中存储的数据得到的结果看起来像是乱码。实现存储引擎本身的接口并不需要了解这个格式。

### 查询引擎

查询引擎部分实现了以下功能：

* 各种函数、聚合算子、算法的实现
* 表单数据结构的定义（schema）
* 数据库查询事务（transaction）
* 查询语句的编译
* 查询的执行

这部分包含 Cozo 项目的大部分代码。关于查询的执行，文档中[有一整章](https://docs.cozodb.org/zh_CN/latest/execution.html)来详细介绍。

Cozo 的 [Rust API](https://docs.rs/cozo/) 实际上就是查询引擎的公共接口。

### 语言、环境封装

Cozo 的 Rust 以外的所有语言、环境都只是对 Rust API 的进一步封装。例如，在独立服务器（cozo）中，Rust API 被封装为了 HTTP 端点，而在 Cozo-Node 中，同步的Rust API 被封装为基于 JavaScript 运行时的异步 API。

封装 Rust API 不难，如果你想让 Cozo 在其它语言上跑起来可以试试。Rust 有一些现成的库用来与其它语言交互。如果你想用某个语言而没有现成的交互库，我们建议你直接封装 Cozo 的 C 语言 API。官方支持的 Go 库就是这么实现的（通过 cgo）。

## 项目进程

Cozo 一开始预想的功能已经实现得不少了，但是项目仍然年轻得很。欢迎各界朋友使用并提出宝贵意见。

Cozo 1.0 之前的版本不承诺语法、API 的稳定性或存储兼容性。

## 许可证和贡献

Cozo 以 MPL-2.0 或其更高版本授权。如果你有兴趣为该项目贡献代码，请看[这里](CONTRIBUTING.md)。