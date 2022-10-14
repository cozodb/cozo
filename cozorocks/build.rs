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

    #[cfg(feature = "io-uring")]
    if target.contains("linux") {
        pkg_config::probe_library("liburing")
            .expect("The io-uring feature was requested but the library is not available");
        builder.define("ROCKSDB_IOURING_PRESENT", Some("1"));
    }

    if target.contains("windows") {
        link("rpcrt4", false);
        link("shlwapi", false);
        builder.define("DWIN32", None);
        builder.define("OS_WIN", None);
        builder.define("_MBCS", None);
        builder.define("WIN64", None);
        builder.define("NOMINMAX", None);
        builder.define("ROCKSDB_WINDOWS_UTF8_FILENAMES", None);

        if &target == "x86_64-pc-windows-gnu" {
            // Tell MinGW to create localtime_r wrapper of localtime_s function.
            builder.define("_POSIX_C_SOURCE", Some("1"));
            // Tell MinGW to use at least Windows Vista headers instead of the ones of Windows XP.
            // (This is minimum supported version of rocksdb)
            builder.define("_WIN32_WINNT", Some("_WIN32_WINNT_VISTA"));
        }
    }

    builder.compile("cozorocks");
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-lib=static=zstd");
    println!("cargo:rustc-link-lib=static=lz4");
    if cfg!(feature = "lib-uring") {
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

fn link(name: &str, bundled: bool) {
    let target = var("TARGET").unwrap();
    let target: Vec<_> = target.split('-').collect();
    if target.get(2) == Some(&"windows") {
        println!("cargo:rustc-link-lib=dylib={}", name);
        if bundled && target.get(3) == Some(&"gnu") {
            let dir = var("CARGO_MANIFEST_DIR").unwrap();
            println!("cargo:rustc-link-search=native={}/{}", dir, target[0]);
        }
    }
}