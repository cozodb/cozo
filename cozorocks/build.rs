/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::env::var;
use std::path::{Path, PathBuf};
use std::{env, fs, process::Command};

fn main() {
    let target = env::var("TARGET").unwrap();

    let mut builder = cxx_build::bridge("src/bridge/mod.rs");
    builder
        .files(["bridge/status.cpp", "bridge/db.cpp", "bridge/tx.cpp"])
        .include(rocksdb_include_dir())
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

    if !Path::new("rocksdb/AUTHORS").exists() {
        update_submodules();
    }

    builder.compile("cozorocks");
    println!("cargo:rustc-link-lib=static=rocksdb");
    println!("cargo:rustc-link-lib=static=zstd");
    println!("cargo:rustc-link-lib=static=lz4");
    if cfg!(feature = "io-uring") {
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

    if !try_to_find_and_link_lib("ROCKSDB") {
        println!("cargo:rerun-if-changed=rocksdb/");
        fail_on_empty_directory("rocksdb");
        build_rocksdb();
    } else {
        let target = env::var("TARGET").unwrap();
        // according to https://github.com/alexcrichton/cc-rs/blob/master/src/lib.rs#L2189
        if target.contains("apple") || target.contains("freebsd") || target.contains("openbsd") {
            println!("cargo:rustc-link-lib=dylib=c++");
        } else if target.contains("linux") {
            println!("cargo:rustc-link-lib=dylib=stdc++");
        }
    }

    // Allow dependent crates to locate the sources and output directory of
    // this crate. Notably, this allows a dependent crate to locate the RocksDB
    // sources and built archive artifacts provided by this crate.
    println!(
        "cargo:cargo_manifest_dir={}",
        env::var("CARGO_MANIFEST_DIR").unwrap()
    );
    println!("cargo:out_dir={}", env::var("OUT_DIR").unwrap());
}

fn link(name: &str, bundled: bool) {
    let target = var("TARGET").unwrap();
    let target: Vec<_> = target.split('-').collect();
    if target.get(2) == Some(&"windows") {
        println!("cargo:rustc-link-lib=dylib={name}");
        if bundled && target.get(3) == Some(&"gnu") {
            let dir = var("CARGO_MANIFEST_DIR").unwrap();
            println!("cargo:rustc-link-search=native={}/{}", dir, target[0]);
        }
    }
}

fn fail_on_empty_directory(name: &str) {
    if fs::read_dir(name).unwrap().count() == 0 {
        println!("The `{name}` directory is empty, did you forget to pull the submodules?");
        println!("Try `git submodule update --init --recursive`");
        panic!();
    }
}

fn rocksdb_include_dir() -> String {
    match env::var("ROCKSDB_INCLUDE_DIR") {
        Ok(val) => val,
        Err(_) => "rocksdb/include".to_string(),
    }
}

fn build_rocksdb() {
    let base = env::var_os("OUT_DIR").unwrap();
    let mut out_lib_path = PathBuf::from(base);
    out_lib_path.push("librocksdb.a");
    if out_lib_path.exists() {
        return;
    }

    let target = env::var("TARGET").unwrap();

    let mut config = cc::Build::new();
    config.include("rocksdb/include/");
    config.include("rocksdb/");
    config.include("rocksdb/third-party/gtest-1.8.1/fused-src/");

    config.define("LZ4", Some("1"));
    if let Some(path) = env::var_os("DEP_LZ4_INCLUDE") {
        config.include(path);
    }

    config.define("ZSTD", Some("1"));
    if let Some(path) = env::var_os("DEP_ZSTD_INCLUDE") {
        config.include(path);
    }

    config.include(".");
    config.define("NDEBUG", Some("1"));

    let mut lib_sources = include_str!("rocksdb_lib_sources.txt")
        .trim()
        .split('\n')
        .map(str::trim)
        // We have a pre-generated a version of build_version.cc in the local directory
        .filter(|file| !matches!(*file, "util/build_version.cc"))
        .collect::<Vec<&'static str>>();

    if target.contains("x86_64") {
        // This is needed to enable hardware CRC32C. Technically, SSE 4.2 is
        // only available since Intel Nehalem (about 2010) and AMD Bulldozer
        // (about 2011).
        let target_feature = env::var("CARGO_CFG_TARGET_FEATURE").unwrap();
        let target_features: Vec<_> = target_feature.split(',').collect();
        if target_features.contains(&"sse2") {
            config.flag_if_supported("-msse2");
        }
        if target_features.contains(&"sse4.1") {
            config.flag_if_supported("-msse4.1");
        }
        if target_features.contains(&"sse4.2") {
            config.flag_if_supported("-msse4.2");
            config.define("HAVE_SSE42", Some("1"));
        }
        // Pass along additional target features as defined in
        // build_tools/build_detect_platform.
        if target_features.contains(&"avx2") {
            config.flag_if_supported("-mavx2");
            config.define("HAVE_AVX2", Some("1"));
        }
        if target_features.contains(&"bmi1") {
            config.flag_if_supported("-mbmi");
            config.define("HAVE_BMI", Some("1"));
        }
        if target_features.contains(&"lzcnt") {
            config.flag_if_supported("-mlzcnt");
            config.define("HAVE_LZCNT", Some("1"));
        }
        if !target.contains("android") && target_features.contains(&"pclmulqdq") {
            config.define("HAVE_PCLMUL", Some("1"));
            config.flag_if_supported("-mpclmul");
        }
    }

    if target.contains("apple-ios") {
        config.define("OS_MACOSX", None);

        config.define("IOS_CROSS_COMPILE", None);
        config.define("PLATFORM", "IOS");
        config.define("NIOSTATS_CONTEXT", None);
        config.define("NPERF_CONTEXT", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);

        env::set_var("IPHONEOS_DEPLOYMENT_TARGET", "11.0");
    } else if target.contains("darwin") {
        config.define("OS_MACOSX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("android") {
        config.define("OS_ANDROID", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("linux") {
        config.define("OS_LINUX", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("freebsd") {
        config.define("OS_FREEBSD", None);
        config.define("ROCKSDB_PLATFORM_POSIX", None);
        config.define("ROCKSDB_LIB_IO_POSIX", None);
    } else if target.contains("windows") {
        link("rpcrt4", false);
        link("shlwapi", false);
        config.define("DWIN32", None);
        config.define("OS_WIN", None);
        config.define("_MBCS", None);
        config.define("WIN64", None);
        config.define("NOMINMAX", None);
        config.define("ROCKSDB_WINDOWS_UTF8_FILENAMES", None);

        if &target == "x86_64-pc-windows-gnu" {
            // Tell MinGW to create localtime_r wrapper of localtime_s function.
            config.define("_POSIX_C_SOURCE", Some("1"));
            // Tell MinGW to use at least Windows Vista headers instead of the ones of Windows XP.
            // (This is minimum supported version of rocksdb)
            config.define("_WIN32_WINNT", Some("_WIN32_WINNT_VISTA"));
        }

        // Remove POSIX-specific sources
        lib_sources = lib_sources
            .iter()
            .cloned()
            .filter(|file| {
                !matches!(
                    *file,
                    "port/port_posix.cc"
                        | "env/env_posix.cc"
                        | "env/fs_posix.cc"
                        | "env/io_posix.cc"
                )
            })
            .collect::<Vec<&'static str>>();

        // Add Windows-specific sources
        lib_sources.extend([
            "port/win/env_default.cc",
            "port/win/port_win.cc",
            "port/win/xpress_win.cc",
            "port/win/io_win.cc",
            "port/win/win_thread.cc",
            "port/win/env_win.cc",
            "port/win/win_logger.cc",
        ]);

        if cfg!(feature = "jemalloc") {
            lib_sources.push("port/win/win_jemalloc.cc");
        }
    }

    config.define("ROCKSDB_SUPPORT_THREAD_LOCAL", None);

    if cfg!(feature = "jemalloc") {
        config.define("WITH_JEMALLOC", "ON");
    }

    #[cfg(feature = "io-uring")]
    if target.contains("linux") {
        pkg_config::probe_library("liburing")
            .expect("The io-uring feature was requested but the library is not available");
        config.define("ROCKSDB_IOURING_PRESENT", Some("1"));
    }

    if target.contains("msvc") {
        config.flag("-EHsc");
        config.flag("-std:c++17");
    } else {
        config.flag(&cxx_standard());
        // matches the flags in CMakeLists.txt from rocksdb
        config.define("HAVE_UINT128_EXTENSION", Some("1"));
        config.flag("-Wsign-compare");
        config.flag("-Wshadow");
        config.flag("-Wno-unused-parameter");
        config.flag("-Wno-unused-variable");
        config.flag("-Woverloaded-virtual");
        config.flag("-Wnon-virtual-dtor");
        config.flag("-Wno-missing-field-initializers");
        config.flag("-Wno-strict-aliasing");
        config.flag("-Wno-invalid-offsetof");
    }

    for file in lib_sources {
        config.file(&format!("rocksdb/{file}"));
    }

    config.file("build_version.cc");

    config.cpp(true);
    config.flag_if_supported("-std=c++17");
    config.compile("librocksdb.a");
}

fn try_to_find_and_link_lib(lib_name: &str) -> bool {
    println!("cargo:rerun-if-env-changed={lib_name}_COMPILE");
    if let Ok(v) = env::var(format!("{lib_name}_COMPILE")) {
        if v.to_lowercase() == "true" || v == "1" {
            return false;
        }
    }

    println!("cargo:rerun-if-env-changed={lib_name}_LIB_DIR");
    println!("cargo:rerun-if-env-changed={lib_name}_STATIC");

    if let Ok(lib_dir) = env::var(format!("{lib_name}_LIB_DIR")) {
        println!("cargo:rustc-link-search=native={lib_dir}");
        let mode = match env::var_os(format!("{lib_name}_STATIC")) {
            Some(_) => "static",
            None => "dylib",
        };
        println!("cargo:rustc-link-lib={}={}", mode, lib_name.to_lowercase());
        return true;
    }
    false
}

fn cxx_standard() -> String {
    env::var("ROCKSDB_CXX_STD").map_or("-std=c++17".to_owned(), |cxx_std| {
        if !cxx_std.starts_with("-std=") {
            format!("-std={cxx_std}")
        } else {
            cxx_std
        }
    })
}

fn update_submodules() {
    let program = "git";
    let dir = "../";
    let args = ["submodule", "update", "--init"];
    println!(
        "Running command: \"{} {}\" in dir: {}",
        program,
        args.join(" "),
        dir
    );
    let ret = Command::new(program).current_dir(dir).args(args).status();

    match ret.map(|status| (status.success(), status.code())) {
        Ok((true, _)) => (),
        Ok((false, Some(c))) => panic!("Command failed with error code {c}"),
        Ok((false, None)) => panic!("Command got killed"),
        Err(e) => panic!("Command failed with error: {e}"),
    }
}
