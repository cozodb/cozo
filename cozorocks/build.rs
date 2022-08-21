use std::env::var;

fn main() {
    let target = var("TARGET").unwrap();

    let mut builder = cxx_build::bridge("src/bridge/mod.rs");
    builder
        .files(["bridge/status.cpp", "bridge/db.cpp", "bridge/tx.cpp"])
        .include("deps/rocksdb/include")
        .include("bridge");
    if target.contains("msvc") {
        builder.flag_if_supported("-std:c++17");
    } else {
        builder.flag_if_supported("-std=c++17");
    };
    builder.compile("cozorocks");

    let manifest_dir = var("CARGO_MANIFEST_DIR").unwrap();

    println!("cargo:rustc-link-search={}/deps/rocksdb/cmake_build/", manifest_dir);
    println!("cargo:rustc-link-search={}/deps/zstd/lib/", manifest_dir);
    println!("cargo:rustc-link-search={}/deps/lz4/lib/", manifest_dir);
    println!("cargo:rustc-link-search={}/deps/jemalloc/lib/", manifest_dir);
    println!("cargo:rustc-link-lib=static=zstd");
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-lib=static=lz4");
    println!("cargo:rustc-link-lib=static=jemalloc");
    // println!("cargo:rustc-link-lib=z");
    //
    // if target.contains("msvc") {
    //     println!("cargo:rustc-link-lib=rpcrt4");
    //     println!("cargo:rustc-link-lib=shlwapi");
    // } else if target.contains("darwin") {
    //     println!("cargo:rustc-link-lib=bz2");
    // }
    //
    // println!("cargo:rustc-link-lib=snappy");
    println!("cargo:rerun-if-changed=src/bridge/mod.rs");
    println!("cargo:rerun-if-changed=bridge/bridge.h");
    println!("cargo:rerun-if-changed=bridge/common.h");
    println!("cargo:rerun-if-changed=bridge/db.h");
    println!("cargo:rerun-if-changed=bridge/db.cpp");
    println!("cargo:rerun-if-changed=bridge/slice.h");
    println!("cargo:rerun-if-changed=bridge/status.h");
    println!("cargo:rerun-if-changed=bridge/status.cpp");
    println!("cargo:rerun-if-changed=bridge/opts.h");
    println!("cargo:rerun-if-changed=bridge/iter.h");
    println!("cargo:rerun-if-changed=bridge/tx.h");
    println!("cargo:rerun-if-changed=bridge/tx.cpp");
}
