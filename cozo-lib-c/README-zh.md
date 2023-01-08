# Cozo C 语言库

[![C](https://img.shields.io/github/v/release/cozodb/cozo)](https://github.com/cozodb/cozo/releases)

这里是 Cozo 的 C API 的源代码。

本文叙述的是如何安装设置 Cozo 的 C 语言库。有关如何使用 CozoDB（CozoScript）的信息，见 [文档](https://docs.cozodb.org/zh_CN/latest/index.html) 。

预编译的库可从 [GitHub 发布页面](https://github.com/cozodb/cozo/releases) 下载，其中 C 语言库以 `libcozo_c` 开头。

C 语言的 API 在这个 [头文件](./cozo_c.h) 中。

[这个程序](./example.c) 举例说明了如何调用此 API，程序可使用以下命令编译安装：

```bash
gcc -L../target/release/ -lcozo_c example.c -o example && ./example
```

# 从源码编译

首先需要安装 [Rust 工具链](https://www.rust-lang.org/tools/install) ，然后：

```bash
cargo build --release -p cozo_c -F compact -F storage-rocksdb
```
