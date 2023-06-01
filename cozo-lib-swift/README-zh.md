# Cozo Swift 库（仅支持苹果硬件）

[![pod](https://img.shields.io/cocoapods/v/CozoSwiftBridge)](https://github.com/cozodb/cozo/tree/main/cozo-lib-swift)

本文叙述的是如何安装设置库本身。有关如何使用 CozoDB（CozoScript）的信息，见 [文档](https://docs.cozodb.org/zh_CN/latest/index.html) 。

本库可在 macOS（苹果 ARM 或 Intel 芯片）以及 iOS（iPad、iPhone、模拟器）中使用。

预编译的二进制包里，持久化引擎中，只有 `storage-sqlite` 在此库中可用。如果需要其他引擎请从源码编译。

## 安装

### CocoaPods

```ruby
target 'YourApp' do
  use_frameworks!

  pod 'CozoSwiftBridge', '~> 0.7.2'
end
```

### Swift Package Manager (SPM)

从 [GitHub 发布页面](https://github.com/cozodb/cozo/releases) 下载名为 `CozoSwiftBridge.tgz` 的包，然后手动导入至 XCode 中。详见 [英文文档](./README.md)。

## 基本使用

```swift
import CozoSwiftBridge

{
    let path = NSHomeDirectory()
    let file = path + "/cozo-data.db"
    let db = CozoDB("sqlite", file)
    let res = try! db.run("?[] <- [[1,2,3]]").toString()
}
```

上例中创建了一个 SQLite 引擎支持的数据库。如果要用纯内存引擎：

```swift
let db = CozoDB()
```

### API

```
public class CozoDB {
    public let db: DbInstance

    /**
    * 构造一个纯内存引擎的数据库
    */
    public init();

    /**
    * 构造一个数据库
    *
    * `kind`: 引擎类型，`mem` 或 `sqlite`。
    * `path`: 存储文件的路径，仅在 `sqlite` 引擎下有效。
    */
    public init(kind: String, path: String) throws;
    
    /**
     * 执行查询文本
     *
     * `query`:   查询文本
     */
    public func run(_ query: String) throws -> [NamedRow];
        
    /**
     * 执行查询文本
     *
     * `query`:   查询文本
     * `params`:  文本中可用的参数
     */
    public func run(_ query: String, params: JSON) throws -> [NamedRow];
    
    /**
     * 导出纯出表至 JSON
     *
     * `relations`: 需导出的表名
     */
    public func exportRelations(relations: [String]) throws -> JSON;
    
    /**
     * 导入数据至存储表中
     * 
     * 注意此方法不会激活任何触发器。
     * 
     * `data`: 导入内容，与 `exportRelations` 返回的格式相同 
     */
    public func importRelations(data: JSON) throws;
   
    /**
     * 备份数据库
     *
     * `path`: 备份路径
     */
    public func backup(path: String) throws;
    
    /**
     * 将备份恢复到当前数据库
     *
     * `path`: 备份路径
     */
    public func restore(path: String) throws;
    
    /**
     * 将备份中表里的数据插入当前数据库中选定的同名表中
     *
     * 注意此方法不会激活任何触发器。
     *
     * `path`:      备份路径
     * `relations`: 需要导入数据的表名
     */
    public func importRelationsFromBackup(path: String, relations: [String]) throws;
}
```

更多信息 [见此](https://docs.cozodb.org/zh_CN/latest/nonscript.html) 。

## 编译

首先安装 [Rust 工具链](https://rustup.rs)。然后执行 [此批处理文件](build-rust.sh) 。建议执行时设置环境变量`CARGO_PROFILE_RELEASE_LTO=fat`：编译时间会变长，但是生成的库更快。

如果一切都没问题，则 `CozoSwiftBridge` 文件夹里会有编译好的文件。

如果想使用 RocksDB 引擎，则在批处理文件中，将以下两行
```bash
cargo build -p cozo-swift -F compact --target x86_64-apple-darwin --release
cargo build -p cozo-swift -F compact --target aarch64-apple-darwin --release
```
改为
```bash
cargo build -p cozo-swift -F compact -F storage-rocksdb --target x86_64-apple-darwin --release
cargo build -p cozo-swift -F compact -F storage-rocksdb --target aarch64-apple-darwin --release
```
注意，给 iOS 编译 RocksDB 不是一件简单的事情。

在使用生成的库时，需要在 XCode 中选择链接至 `libc++` 动态库。