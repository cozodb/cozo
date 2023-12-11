$ErrorActionPreference="Stop"

$VERSION = cat .\VERSION
$env:CARGO_PROFILE_RELEASE_LTO = "fat"
$TARGET = "x86_64-pc-windows-msvc"
# $env:PYO3_NO_PYTHON = 1

mkdir -force release > $null

cd cozo-lib-python
maturin build -F compact -F storage-rocksdb --release --strip --target $TARGET
cd ..

cargo build --release -p cozo-bin -p cozo_c -p cozo_java -p cozo-node -F compact -F storage-rocksdb --target $TARGET
cp target/$TARGET/release/cozo-bin.exe release/cozo-$VERSION-$TARGET.exe # standalone
cp target/$TARGET/release/cozo_c.lib release/libcozo_c-$VERSION-$TARGET.lib # c static
cp target/$TARGET/release/cozo_c.dll release/libcozo_c-$VERSION-$TARGET.dll # c dynamic
cp target/$TARGET/release/cozo_java.dll release/libcozo_java-$VERSION-$TARGET.dll # java
cp target/$TARGET/release/cozo_node.dll release/libcozo_node-$VERSION-$TARGET.dll # nodejs

cp target/wheels/*.whl release/

# $TARGET = "x86_64-pc-windows-gnu"
# cargo build --release -p cozo-bin -p cozo_c -p cozo_java -p cozo-node -F compact -F storage-rocksdb --target $TARGET
# cp target/$TARGET/release/cozo-bin.exe release/cozo-$VERSION-$TARGET.exe # standalone
# cp target/$TARGET/release/libcozo_c.a release/libcozo_c-$VERSION-$TARGET.a # c static
# cp target/$TARGET/release/cozo_c.dll release/libcozo_c-$VERSION-$TARGET.dll # c dynamic
# cp target/$TARGET/release/cozo_java.dll release/libcozo_java-$VERSION-$TARGET.dll # java
# cp target/$TARGET/release/cozo_node.dll release/libcozo_node-$VERSION-$TARGET.dll # nodejs
