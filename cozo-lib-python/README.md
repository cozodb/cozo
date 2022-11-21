# cozo-lib-python

[![pypi](https://img.shields.io/pypi/v/cozo_embedded)](https://pypi.org/project/cozo_embedded/)

Native bindings for embedding [CozoDB](https://github.com/cozodb/cozo) in Python, providing the
`cozo_embedded` package.

You are not supposed to be using this package directly in your code. Use [PyCozo](https://github.com/cozodb/pycozo),
which depends on this package.

To build this package, you need to install the Rust toolchain
as well as the [maturin](https://github.com/PyO3/maturin) python package.
Refer maturin's docs for how to [develop](https://www.maturin.rs/develop.html) 
and [build](https://www.maturin.rs/distribution.html) this package.