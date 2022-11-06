# Building Cozo from source

You need to install the [Rust toolchain](https://www.rust-lang.org/tools/install) on your system.
You also need a C++17 compiler.

Clone the Cozo git repo:

```bash
git clone https://github.com/cozodb/cozo.git --recursive
```

You need to pass the `--recursive` flag so that submodules are also cloned. 
Next, run in the root of the cloned repo:

```bash
cargo build --release
```

Wait for potentially a long time, and you will find the compiled binary in `target/release`.

You can run `cargo build --release -F jemalloc` instead
to indicate that you want to compile and use jemalloc as the memory allocator for the RocksDB storage backend,
which can make a difference in performance depending on your workload.

To build the C library:

```bash
cargo build --release --manifest-path=cozo-lib-c/Cargo.toml
```

To build the Java library used by [cozo-lib-java](https://github.com/cozodb/cozo-lib-java):

```bash
cargo build --release --manifest-path=cozo-lib-java/Cargo.toml
```