fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("bridge/cozorocks.cc")
        .include("../deps/include")
        .include("bridge")
        .flag_if_supported("-std=c++17")
        .compile("cozorocks");

    println!("cargo:rustc-link-search=deps/lib/");
    println!("cargo:rustc-link-lib=rocksdb");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=bz2");
    println!("cargo:rustc-link-lib=lz4");
    println!("cargo:rustc-link-lib=snappy");
    println!("cargo:rustc-link-lib=zstd");
    println!("cargo:rerun-if-changed=src/main.rs");
    println!("cargo:rerun-if-changed=bridge/cozorocks.cc");
    println!("cargo:rerun-if-changed=bridge/cozorocks.h");
}