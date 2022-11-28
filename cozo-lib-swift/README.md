# Cozo for Swift on Apple Hardware

This document describes how to set up the Cozo module for use in Swift on Apple hardware.
To learn how to use CozoDB (CozoScript), follow
the [tutorial](https://nbviewer.org/github/cozodb/cozo-docs/blob/main/tutorial/tutorial.ipynb)
first and then read the [manual](https://cozodb.github.io/current/manual/). You can run all the queries
described in the tutorial with an in-browser DB [here](https://cozodb.github.io/wasm-demo/).

This package can be used for MacOS (both Apple ARM and Intel) and iOS (iPad, iPhone and simulators).

Only the `storage-sqlite` engine is enabled for the Swift prebuilt binaries, as using
other storage engines on desktop or mobile does not make too much sense. If you disagree,
see the Building section below.

## Installation

The package is published as an archive containing a Swift package.
Download it from the [release page] (look for `CozoSwiftBridge-<VERSION>.tgz`).
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

## Using the library

```swift
import CozoSwiftBridge

...

let path = NSHomeDirectory()
let file = path + "/cozo-data.db"
let db = new_cozo_db("sqlite", file, "");
let result: String! = db?.run_script_str("::relations", "").toString();
```
Above we created an SQLite-based database. For memory-based ones:
```swift
let db = new_cozo_db("mem", "", "");
```

## API

The function `new_cozo_db` can be used to create a database, passing in the engine type,
the storage path, and options.
The following methods are available on the returned database object:
```
extension DbInstanceRef {
    /**
     * Run query against a database.
     *
     * `payload`: a UTF-8 encoded C-string for the CozoScript to execute.
     * `params`:  a UTF-8 encoded C-string for the params of the query,
     *            in JSON format. You must always pass in a valid JSON map,
     *            even if you do not use params in your query
     *            (pass "{}" in this case).
     */
    public func run_script_str(_ payload: GenericToRustStr, _ params: GenericToRustStr) -> RustString;
    
    /**
     * Import data into relations
     * `data`: a UTF-8 encoded JSON payload, see the manual for the expected fields.
     */
    public func import_relations_str(_ data: GenericToRustStr) -> RustString;
    
    /**
     * Export relations into JSON
     *
     * `data`: a UTF-8 encoded JSON payload, , in the same form as returned by exporting relations
     */
    public func export_relations_str(_ data: GenericToRustStr) -> RustString;
   
    /**
     * Backup the database.
     *
     * `out_file`: path of the output file.
     */
    public func backup_db_str(_ out_file: GenericToRustStr) -> RustString;
    
    /**
     * Restore the database from a backup.
     *
     * `in_file`: path of the input file.
     */
    public func restore_backup_str(_ in_file: GenericToRustStr) -> RustString;
    
    /**
     * Import data into a relation
     *
     * `data`: a UTF-8 encoded JSON payload: `{"path": ..., "relations": [...]}`
     */
    public func import_from_backup_str(_ data: GenericToRustStr) -> RustString;
}
```
You can pass Swift strings as arguments. The returned types are all `RustString`:
you need to call `.toString()` on them to convert to Swift strings, and then parse them
as JSON objects (for example, by using [SwiftyJSON](https://github.com/SwiftyJSON/SwiftyJSON).

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

## Calling for help

We are no experts on Swift/iOS development. In fact, we learned Swift just enough to produce
this package. Everything from packaging, distribution to the ergonomics of the Swift API is
far from ideal, but is as good as we can produce now. If you like Cozo and you can improve 
its user experience on Swift, feel free to open an issue for discussion, or file a pull request.

By the way, the reason that we did not publish the Swift package directly to a GitHub repo is
that the package contains very large binary artefacts, and GitHub will not like it if we put it
directly in the repo.