$ErrorActionPreference="Stop"

if (test-path native/6/index.node) {
    Remove-Item native/6/index.node
}

cargo build --release
cp target/release/cozo_node.dll native/6/index.node
yarn package
