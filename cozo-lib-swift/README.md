# Cozo for Swift on Apple

Only the `storage-sqlite` engine is enabled for the Swift prebuilt binaries, as using
other storage engines on desktop or mobile does not make too much sense. If you disagree,
see the Building section below.

## Using the library

TODO

## Building

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