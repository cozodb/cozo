# Cozo NodeJS 库

[![cozo-node](https://img.shields.io/npm/v/cozo-node)](https://www.npmjs.com/package/cozo-node)

NodeJS 的嵌入式 [CozoDB](https://www.cozodb.org) 库。

本文叙述的是如何安装设置库本身。有关如何使用 CozoDB（CozoScript）的信息，见 [文档](https://docs.cozodb.org/zh_CN/latest/index.html) 。

## 安装

```bash
npm install --save cozo-node
```

安装过程中需要从 [GitHub 发布页](https://github.com/cozodb/cozo-lib-nodejs/releases/tag/0.4.1) 下载二进制文件。如果因为网络问题失败，可以使用此 [镜像](https://github.com/cozodb/cozo-lib-nodejs/releases/tag/0.4.1) ，使用方法为

```bash
npm install --save cozo-node --cozo_node_prebuilt_binary_host_mirror=https://gitee.com/cozodb/cozo-lib-nodejs/releases/download/
```

注意：如果你用的是 Yarn 而不是 NPM，类似的命令 [可能不奏效](https://github.com/mapbox/node-pre-gyp/issues/514) 。

如果你的操作系统、平台不是常见的平台，可能会报错说找不到预编译库。这种情况下可以参见后面关于如何从源码编译的内容。

## 用法

```javascript
const {CozoDb} = require('cozo-node')

const db = new CozoDb()

function printQuery(query, params) {
    db.run(query, params)
        .then(data => console.log(data))
        .catch(err => console.error(err.display || err.message))
}

printQuery("?[] <- [['hello', 'world!']]")
printQuery("?[] <- [['hello', 'world', $name]]", {"name": "JavaScript"})
printQuery("?[a] <- [[1, 2]]")
```

### API

```ts
class CozoDb {
    /**
     * 构造函数
     * 
     * @param engine:  默认为 'mem'，即纯内存的非持久化存储。其他值可以是 'sqlite'、'rocksdb' 等
     * @param path:    存储文件或文件夹的路径，默认为 'data.db'。在 'mem' 引擎下无用。
     * @param options: 默认为 {}，在 NodeJS 支持的引擎中无用。
     */
    constructor(engine: string, path: string, options: object): CozoDb;

    /**
     * 关闭数据库，并释放其原生资源。如果不调用此方法而直接删除数据库的变量，则会造成原生资源泄漏。
     */
    close(): void;

    /**
     * 执行查询文本
     * 
     * @param script: 查询文本
     * @param params: 传入的参数，默认为 {}
     */
    async run(script: string, params: object): object;

    /**
     * 导出存储表
     * 
     * @param relations:  需要导出的存储表名称
     */
    async exportRelations(relations: Array<string>): object;

    /**
     * 导入数据至存储表
     * 
     * 注意：以此方法导入数据不会激活存储表上任何的触发器。
     * 
     * @param data: 导入的表以及数据，格式与 `exportRelations` 返回的相同
     */
    async importRelations(data: object): object;

    /**
     * 备份数据库
     * 
     * @param path: 备份文件路径
     */
    async backup(path: string): object;

    /**
     * 从备份文件恢复数据至当前数据库。若当前数据库非空，则报错。
     * 
     * @param path: 备份文件路径
     */
    async restore(path: string): object;

    /**
     * 将备份文件中指定存储表里的数据插入当前数据库中同名表里。
     *
     * 注意：以此方法导入数据不会激活存储表上任何的触发器。
     *
     * @param path: 备份文件路径
     * @param rels: 需导入数据的表名
     */
    async importRelationsFromBackup(path: string, rels: Array<string>): object;
}
```

更多信息 [见此](https://docs.cozodb.org/zh_CN/latest/nonscript.html) 。

## 编译

编译 `cozo-node` 需要 [Rust 工具链](https://rustup.rs)。运行

```bash
cargo build --release -p cozo-node -F compact -F storage-rocksdb
```

完成后，动态链接库可以在 `../target/` 文件夹中找到（具体文件名根据平台与操作系统会有差异，一般来说 Linux 上 扩展名为 `.so`，Mac 上为 `.dylib`，Windows 上为 `.dll`）。
将找到的动态库拷贝为此目录下的 `native/6/cozo_node_prebuilt.node` 文件（中间目录若不存在，则需建立）。

如果一切操作正确，在此目录下执行下列命令则会正常返回：

```bash
node example.js
```