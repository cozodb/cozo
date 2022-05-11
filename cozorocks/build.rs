fn main() {
    cxx_build::bridge("src/bridge.rs")
        .file("bridge/cozorocks.cc")
        .include("../deps/include")
        .include("bridge")
        .flag_if_supported("-std=c++17")
        .compile("cozorocks-cxx");

    // let mut b = autocxx_build::Builder::new(
    //     "src/bridge.rs",
    //     &["../deps/include", "bridge"])
    //     .extra_clang_args(&["-std=c++17"])
    //     .build()?;
    // // This assumes all your C++ bindings are in main.rs
    // b.flag_if_supported("-std=c++17")
    //     .compile("cozorocks-autocxx"); // arbitrary library name, pick anything
    println!("cargo:rustc-link-search=deps/lib/");
    println!("cargo:rustc-link-search=/opt/homebrew/lib/");
    println!("cargo:rustc-link-lib=rocksdb");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=bz2");
    println!("cargo:rustc-link-lib=lz4");
    println!("cargo:rustc-link-lib=snappy");
    println!("cargo:rustc-link-lib=zstd");
    println!("cargo:rustc-link-lib=jemalloc");
    println!("cargo:rerun-if-changed=src/bridge.rs");
    println!("cargo:rerun-if-changed=bridge/cozorocks.cc");
    println!("cargo:rerun-if-changed=bridge/cozorocks.h");
}
