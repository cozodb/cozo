fn main() {
    cxx_build::bridge("src/lib.rs")
        .file("src/cozorocks.cc")
        .include("../rocksdb/include")
        .include("src")
        .flag_if_supported("-std=c++17")
        .compile("cozo-rocks");

    println!("cargo:rustc-link-search=rocksdb/");
    println!("cargo:rustc-link-lib=rocksdb");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=bz2");
    println!("cargo:rerun-if-changed=src/main.rs");
    println!("cargo:rerun-if-changed=src/cozorocks.cc");
    println!("cargo:rerun-if-changed=src/cozorocks.h");
}