# Cozo Java语言库

This crate provides the JNI bindings for using Cozo in Java/JVM languages/Android.

You do not use this crate directly. Instead, use:

* ... for Java or other JVM languages
* ... for Clojure on JVM (you can also use the Java library, but this one is nicer)
* ... for Android

Keep reading only if the prebuilt binaries provided by these libraries do not suit your needs.

## Building for JDK

With the Rust toolchain installed,
```bash
cargo build --release -p cozo_java -F storage-rocksdb
```

## Building for Android

Building for Android is not easy, and we will be very sketchy.

The first thing to note is that you should omit `-F storage-rocksdb` from the build command above,
unless you are prepared to manually change lots of `build.rs` flags in 
[cozorocks](../cozorocks) to build the RocksDB dependency.

Then, in addition to adding Android targets to the Rust toolchain, 
you also need to set up the Android NDK
cross-compilation and libraries paths, etc.
This is notoriously hard to get right, but fortunately 
you can just use the Docker image [here](https://github.com/cross-rs/cross)
which has everything set up for you.

When everything is set up correctly, the following command show complete without errors:

```bash
for TARGET in aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android; do
  cross build -p cozo_java --release --target=$TARGET
done
```

For running on modern Android phones, the single target `aarch64-linux-android` is probably enough.