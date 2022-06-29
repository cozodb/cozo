fn main() -> miette::Result<()> {
    let mut b = autocxx_build::Builder::new("src/lib.rs", &["../deps/include", "src"]).build()?;
    b.flag_if_supported("-std=c++17").compile("cozorocks"); // arbitrary library name, pick anything

    println!("cargo:rustc-link-search=../deps/lib/");
    println!("cargo:rustc-link-search=/opt/homebrew/lib/");
    println!("cargo:rustc-link-lib=rocksdb");
    println!("cargo:rustc-link-lib=z");
    println!("cargo:rustc-link-lib=bz2");
    println!("cargo:rustc-link-lib=lz4");
    println!("cargo:rustc-link-lib=snappy");
    println!("cargo:rustc-link-lib=zstd");
    println!("cargo:rustc-link-lib=jemalloc");
    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=src/bridge.h");
    Ok(())
}
