# Cozo Java 语言库

这里是 Cozo 的 JNI 接口库，可以在 Java/JVM 语言/安卓中使用。

一般情况下，这个库不是直接使用的。用于应当使用以下调用此库的库：

* [Cozo Java](https://github.com/cozodb/cozo-lib-java)：在 JVM Java 中使用
* [Cozo Clojure](https://github.com/cozodb/cozo-clj)：在 JVM Clojure 中使用
* [Cozo Android](https://github.com/cozodb/cozo-lib-android)：在安卓中使用

下面几个小节介绍在上面几个库不支持你的平台时如何从源码编译此库。

## 为 JDK 编译

首先安装 Rust 工具链，然后：
```bash
cargo build --release -p cozo_java -F storage-rocksdb
```

## 为安卓编译

为安卓编译较为复杂，以下仅做简要叙述。

首先，在编译时请不要使用 `-F storage-rocksdb` 选项，除非你有能力在 `build.rs` 中做出大量调整使得 [cozorocks](../cozorocks) 能够成功为安卓编译。

然后，在 Rust 工具链中添加安卓目标，设置好安卓 NDK 以及其编译路径、库路径等。手动搞定这些非常复杂，不过 [这里](https://github.com/cross-rs/cross) 有一些系统镜像可以省去你不少工作。

所有上面所述都设置好了之后，执行下面命令就可以编译安卓库了：

```bash
for TARGET in aarch64-linux-android armv7-linux-androideabi i686-linux-android x86_64-linux-android; do
  cross build -p cozo_java --release --target=$TARGET
done
```

上面编译了多个架构的安卓库。如果只是想在常见的安卓手机平板上运行，`aarch64-linux-android` 一个目标其实就够了。