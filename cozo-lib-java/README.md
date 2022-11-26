# Cozo-lib-java

This crate provides the JNI bindings for using Cozo in Java/JVM languages/Android.

You do not use this crate directly. Instead, use:

* ... for Android
* ... for Java or other JVM languages
* ... for Clojure on JVM (you can also use the Java library, but this one is nicer)

## Building

With the Rust toolchain installed,
```bash
cargo build --release  -p cozo_java -F compact -F storage-rocksdb
```