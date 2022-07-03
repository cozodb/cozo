fn main() {
    println!("cargo:rustc-link-search=deps/lib/");
    println!("cargo:rustc-link-search=/opt/homebrew/lib/");
    println!("cargo:rustc-link-lib=rocksdb");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=bz2");
    println!("cargo:rustc-link-lib=lz4");
    println!("cargo:rustc-link-lib=snappy");
    println!("cargo:rustc-link-lib=zstd");
    println!("cargo:rustc-link-lib=jemalloc");
    println!("cargo:rerun-if-changed=cozorocks/src/bridge/mod.rs");
    println!("cargo:rerun-if-changed=cozorocks/bridge/bridge.h");
    println!("cargo:rerun-if-changed=cozorocks/bridge/common.h");
    println!("cargo:rerun-if-changed=cozorocks/bridge/db.h");
    println!("cargo:rerun-if-changed=cozorocks/bridge/db.cpp");
    println!("cargo:rerun-if-changed=cozorocks/bridge/slice.h");
    println!("cargo:rerun-if-changed=cozorocks/bridge/status.h");
    println!("cargo:rerun-if-changed=cozorocks/bridge/status.cpp");
    println!("cargo:rerun-if-changed=cozorocks/bridge/tx.h");

    cxx_build::bridge("src/bridge/mod.rs")
        .files(["bridge/status.cpp", "bridge/db.cpp"])
        .include("../deps/include")
        .include("bridge")
        .flag_if_supported("-std=c++17")
        .compile("cozorocks");
}
