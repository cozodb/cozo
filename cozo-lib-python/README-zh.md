# Cozo Python 库

[![pypi](https://img.shields.io/pypi/v/cozo_embedded)](https://pypi.org/project/cozo_embedded/)

[CozoDB](https://www.cozodb.org) 的 Python 嵌入式库 `cozo_embedded` 的源代码。

一般来说你应该使用 [PyCozo](https://github.com/cozodb/pycozo) 库（ [Gitee 镜像](https://gitee.com/cozodb/pycozo) ），而不是直接使用此库。

编译此库需要安装 Rust 工具链以及 [maturin](https://github.com/PyO3/maturin) 。安装好后运行

```bash
maturin build -F compact -F storage-rocksdb --release
```

更多选项参见 maturin 的 [文档](https://www.maturin.rs/) 。