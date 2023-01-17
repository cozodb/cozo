# Cozo（独立程序）

[![server](https://img.shields.io/github/v/release/cozodb/cozo)](https://github.com/cozodb/cozo/releases)

本文叙述的是如何安装设置 Cozo 的独立程序本身。有关如何使用 CozoDB（CozoScript）的信息，见 [文档](https://docs.cozodb.org/zh_CN/latest/index.html) 。

## 下载

独立服务的程序可以从 [GitHub 发布页](https://github.com/cozodb/cozo/releases) 或 [Gitee 发布页](https://gitee.com/cozodb/cozo/releases) 下载，其中名为 `cozoserver-*` 的是独立服务程序，名为 `cozoserver_all-*` 的独立程序同时支持更多地存储引擎，比如 [TiKV](https://tikv.org/)。

## 启动服务程序

在终端中执行：

```bash
./cozoserver
```

如此执行命令会使用纯内存的非持久化存储引擎。执行 `./cozoserver -h` 可查看如何启用其它引擎，以及其它参数。

若要终止程序，按下 `CTRL-C` 按键，或向进程发送 `SIGTERM` （比如通过 `kill` 命令）。

## 命令行界面

在执行时加入 `-r` 或 `--repl` 参数可开启命令行界面（REPL），同时不会启动 web 服务。其它选择存储引擎的参数可一同使用。

在界面中可以使用以下特殊命令：

* `%set <键> <值>`：设置在查询中可用的参数值。
* `%unset <键>`：删除已设置的参数值。
* `%clear`：清空所有已设置的参数。
* `%params`：显示当前所有参数。
* `%import <文件或 URL>`：将文件或 URL 里的 JSON 数据导入至数据库。
* `%save <文件>`：下一个成功查询的结果将会以 JSON 格式存储在指定的文件中。如果文件参数未给出，则清除上次的文件设置。
* `%backup <文件>`：备份全部数据至指定的文件。
* `%restore <文件>`：将指定的备份文件中的数据加载到当前数据库中。当前数据库必须为空。

## 查询 API

查询通过向 API 发送 POST 请求来完成。默认的请求地址是 `http://127.0.0.1:9070/text-query` 。请求必须包含 JSON 格式的正文，具体内容如下：
```json
{
    "script": "<COZOSCRIPT QUERY STRING>",
    "params": {}
}
```
`params` 给出了查询文本中可用的变量。例如，当 `params` 为 `{"num": 1}` 时，查询文本中可以以 `$num` 来代替常量 `1`。请善用此功能，而不是手动拼接查询字符串。

HTTP API 返回的结果永远是 JSON 格式的。如果请求成功，则返回结果的 `"ok"` 字段将为 `true`，且 `"rows"` 字段将含有查询结果的行，而 `"headers"` 将含有表头。如果查询报错，则 `"ok"` 字段将为 `false`，而错误信息会在 `"message"` 字段中，同时 `"display"` 字段会包含格式化好的友好的错误提示。

> Cozo 的设计，基于其在一个受信任的环境中运行，且其所有用户也是由受信任的这种假设。因此 Cozo 没有内置认证与复杂的安全机制。如果你需要远程访问 Cozo 服务，你必须自己设置防火墙、加密和代理等，用来保护服务器上资源的安全。
> 
> 由于总是会有用户不小心将服务接口暴露于外网，Cozo 有一个补救措施：如果从非回传地址访问 Cozo，则必须在所有请求中以 HTTP 文件头 `x-cozo-auth` 的形式附上访问令牌。访问令牌的内容在启动服务的终端中有提示。注意这仅仅是一个补救措施，并不是特别可靠的安全机制，是为了尽量防止一些不由于小心而造成严重后果的悲剧。

## 所有 API

* `POST /text-query`，见上。
* `GET /export/{relations: String}`，导出指定表中的数据，其中 `relations` 是以逗号分割的表名。
* `PUT /import`，向数据库导入数据。所导入的数据应以在正文中以 `application/json` MIME 类型传入，具体格式与 `/export` 返回值中的 `data` 字段相同。
* `POST /backup`，备份数据库，需要传入 JSON 正文 `{"path": <路径>}`。
* `POST /import-from-backup`，将备份中指定存储表中的数据插入当前数据库中同名存储表。需要传入 JSON 正文 `{"path": <路径>, "relations": <表名数组>}`.
* `GET /`，用浏览器打开这个地址，然后打开浏览器的调试工具，就可以使用一个简陋的 JS 客户端。

> 注意 `import` 与 `import-from-backup` 接口不会激活任何触发器。


## 编译

编译 `cozoserver` 需要 [Rust 工具链](https://rustup.rs)。运行

```bash
cargo build --release -p cozoserver -F compact -F storage-rocksdb
```
