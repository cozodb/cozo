# Cozo for Swift on Apple Hardware

[![pod](https://img.shields.io/cocoapods/v/CozoSwiftBridge)](https://github.com/cozodb/cozo/tree/main/cozo-lib-swift)

This document describes how to set up the Cozo module for use in Swift on Apple hardware.
To learn how to use CozoDB (CozoScript), follow
the [tutorial](https://github.com/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
first and then read the [manual](https://cozodb.github.io/current/manual/). You can run all the queries
described in the tutorial with an in-browser DB [here](https://cozodb.github.io/wasm-demo/).

This package can be used for MacOS (both Apple ARM and Intel) and iOS (iPad, iPhone and simulators).

Only the `storage-sqlite` engine is enabled for the Swift prebuilt binaries, as using
other storage engines on desktop or mobile does not make too much sense. If you disagree,
see the Building section below.

## Installation

### CocoaPods

```ruby
target 'YourApp' do
  use_frameworks!

  pod 'CozoSwiftBridge', '~> 0.4.0'
end
```

### Swift Package Manager (SPM)

The package is published as an archive containing a Swift package.
Download it from the [release page](https://github.com/cozodb/cozo/releases) (look for `CozoSwiftBridge.tgz`).
Uncompress.

In XCode of your project, select from the menu `File > Add Packages`,
select `Add Local ...` on the bottom, choose the folder you just decompressed 
(the one containing a `Package.swift` at the root), then click `Add Package`.

Then click your project on the left pane, and go to 
`General > Frameworks, Libraries, and Embedded Content`,
click on the plus sign, and add `Workspace > CozoSwiftBridge > CozoSwiftBridge` 
(with a library icon).

If you did everything right, you should also see `CozoSwiftBridge` under 
`Build Phases > Link Binary With Libraries`.

> You cannot download swift packages directly from GitHub repo, since
in order to support that we would need to check the binaries
into version control, and GitHub does not like it (we tried to work
this around with Git LFS, but no luck).

## Using the library

```swift
import CozoSwiftBridge

{
    let path = NSHomeDirectory()
    let file = path + "/cozo-data.db"
    let db = CozoDB("sqlite", file)
    let res = try! db.run("?[] <- [[1,2,3]]").toString()
}
```
Above we created an SQLite-based database. For memory-based ones:
```swift
let db = CozoDB()
```

### API

```
public class CozoDB {
    public let db: DbInstance

    /**
    * Constructs an in-memory database.
    */
    public init();

    /**
    * Constructs a database.
    *
    * `kind`: the engine kind, can be `mem` or `sqlite`.
    * `path`: specifies the path to the storage file, only used for `sqlite` engine
    */
    public init(kind: String, path: String) throws;
    
    /**
     * Run query against the database.
     *
     * `query`:   the CozoScript to execute.
     */
    public func run(_ query: String) throws -> [NamedRow];
        
    /**
     * Run query against the database.
     *
     * `query`:   the CozoScript to execute.
     * `params`:  the params of the query in JSON format.
     */
    public func run(_ query: String, params: JSON) throws -> [NamedRow];
    
    /**
     * Export relations as JSON
     *
     * `relations`: the stored relations to export
     */
    public func exportRelations(relations: [String]) throws -> JSON;
    
    /**
     * Import data into relations
     * 
     * Note that triggers are _not_ run for the relations, if any exists.
     * If you need to activate triggers, use queries with parameters.
     * 
     * `data`: the payload, in the same format as returned by `exportRelations`. 
     */
    public func importRelations(data: JSON) throws;
   
    /**
     * Backup the database.
     *
     * `path`: path of the output file.
     */
    public func backup(path: String) throws;
    
    /**
     * Restore the database from a backup.
     *
     * `path`: path of the input file.
     */
    public func restore(path: String) throws;
    
    /**
     * Import data into a relation from a backup.
     *
     * Note that triggers are _not_ run for the relations, if any exists.
     * If you need to activate triggers, use queries with parameters.
     *
     * `path`:      path of the input file.
     * `relations`: the stored relations to import into.
     */
    public func importRelationsFromBackup(path: String, relations: [String]) throws;
}
```

## Building the Swift Package

First, install the [Rust toolchain](https://rustup.rs). 
Then run the [build script](build-rust.sh) in this directory. 
It is recommended to also set the environment variable `CARGO_PROFILE_RELEASE_LTO=fat`:
this makes the building process much longer, but in turn the library runs a little bit faster.

When everything goes well, you should find the compiled Swift package in a directory called
`CozoSwiftBridge`.

If you want to use the RocksDB engine on Desktop, in the build script change the two lines
```bash
cargo build -p cozo-swift -F compact --target x86_64-apple-darwin --release
cargo build -p cozo-swift -F compact --target aarch64-apple-darwin --release
```
to
```bash
cargo build -p cozo-swift -F compact -F storage-rocksdb --target x86_64-apple-darwin --release
cargo build -p cozo-swift -F compact -F storage-rocksdb --target aarch64-apple-darwin --release
```

Then you also need to link your executable with `libc++`: in XCode, click on your project
in the left drawer, then on the right go to `Build phases > Link Binary With Libraries`,
click the plus sign, search for `libc++`, then add `libc++.tbd` found under Apple SDKs.

Similar same process goes if you want to enable other features. Note that building the
RocksDB engine for mobile is a very demanding task!
