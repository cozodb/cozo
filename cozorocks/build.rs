fn main() {
    println!("cargo:rustc-link-search=../deps/lib/");
    println!("cargo:rustc-link-search=/opt/homebrew/lib/");
    println!("cargo:rustc-link-lib=rocksdb");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=bz2");
    println!("cargo:rustc-link-lib=lz4");
    println!("cargo:rustc-link-lib=snappy");
    println!("cargo:rustc-link-lib=zstd");
    println!("cargo:rustc-link-lib=jemalloc");
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

    cxx_build::bridge("src/bridge/mod.rs")
        .files(["bridge/status.cpp", "bridge/db.cpp", "bridge/tx.cpp"])
        .include("../deps/include")
        .include("bridge")
        .flag_if_supported("-std=c++17")
        .compile("cozorocks");
}
