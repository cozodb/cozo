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

    if target.contains("msvc") {
        println!("cargo:rustc-link-search={}/deps/zstd/build/VS2010/bin/x64_Release", manifest_dir);
        println!("cargo:rustc-link-search={}/deps/lz4/build/VS2017/bin/x64_Release", manifest_dir);
        println!("cargo:rustc-link-search={}/deps/rocksdb/cmake_build/Release", manifest_dir);
        println!("cargo:rustc-link-search={}/deps/gflags/cmake_build/lib/Release", manifest_dir);
        println!("cargo:rustc-link-lib=static=rocksdb");
        println!("cargo:rustc-link-lib=static=gflags_static");
        println!("cargo:rustc-link-lib=libzstd");
        println!("cargo:rustc-link-lib=static=liblz4_static");
        println!("cargo:rustc-link-lib=rpcrt4");
        println!("cargo:rustc-link-lib=shlwapi");
    } else {
        println!("cargo:rustc-link-search={}/deps/rocksdb/cmake_build/", manifest_dir);
        println!("cargo:rustc-link-search={}/deps/zstd/lib/", manifest_dir);
        println!("cargo:rustc-link-search={}/deps/lz4/lib/", manifest_dir);
        println!("cargo:rustc-link-search={}/deps/jemalloc/lib/", manifest_dir);
        println!("cargo:rustc-link-lib=static=zstd");
        println!("cargo:rustc-link-lib=static=rocksdb");
        println!("cargo:rustc-link-lib=static=lz4");
        println!("cargo:rustc-link-lib=static=jemalloc");

    }

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
