use std::env::var;

fn main() {
    let target = var("TARGET").unwrap();

    let mut builder = cxx_build::bridge("src/bridge/mod.rs");
    builder
        .files(["bridge/status.cpp", "bridge/db.cpp", "bridge/tx.cpp"])
        .include("rocksdb/include")
        .include("bridge");
    if target.contains("msvc") {
        builder.flag_if_supported("-EHsc");
        builder.flag_if_supported("-std:c++17");
    } else {
        builder.flag(&cxx_standard());
        builder.define("HAVE_UINT128_EXTENSION", Some("1"));
        builder.flag("-Wsign-compare");
        builder.flag("-Wshadow");
        builder.flag("-Wno-unused-parameter");
        builder.flag("-Wno-unused-variable");
        builder.flag("-Woverloaded-virtual");
        builder.flag("-Wnon-virtual-dtor");
        builder.flag("-Wno-missing-field-initializers");
        builder.flag("-Wno-strict-aliasing");
        builder.flag("-Wno-invalid-offsetof");
    };
    builder.compile("cozorocks");
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-lib=static=zstd");
    println!("cargo:rustc-link-lib=static=lz4");
    if cfg!(feature = "snappy") {
        println!("cargo:rustc-link-lib=static=uring");
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

fn cxx_standard() -> String {
    var("ROCKSDB_CXX_STD").map_or("-std=c++17".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std=") {
            format!("-std={}", cxx_std)
        } else {
            cxx_std
        }
    })
}
