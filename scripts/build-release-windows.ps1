$ErrorActionPreference="Stop"

$VERSION = cat .\VERSION
$CARGO_PROFILE_RELEASE_LTO = "fat"
$TARGET = "x86_64-pc-windows-msvc"
$PYO3_NO_PYTHON=1 

mkdir -force release > $null

cargo build --release -p cozoserver -p cozo_c -p cozo_java -p cozo-node -F compact -F storage-rocksdb --target $TARGET
cp target/$TARGET/release/cozoserver.exe release/cozoserver-$VERSION-$TARGET.exe # standalone
cp target/$TARGET/release/cozo_c.lib release/libcozo_c-$VERSION-$TARGET.lib # c static
cp target/$TARGET/release/cozo_c.dll release/libcozo_c-$VERSION-$TARGET.dll # c dynamic
cp target/$TARGET/release/cozo_java.dll release/libcozo_java-$VERSION-$TARGET.dll # java
cp target/$TARGET/release/cozo_node.dll release/libcozo_node-$VERSION-$TARGET.dll # nodejs

cd cozo-lib-python
maturin build -F compact -F storage-rocksdb --release --strip --target $TARGET
cd ..

cp target/wheels/*.whl release/


# cargo build --release
# cargo build --release --manifest-path=cozo-lib-c/Cargo.toml
# cargo build --release --manifest-path=cozo-lib-java/Cargo.toml

# cp target/release/cozoserver.exe release/cozoserver-${COZO_VERSION}-windows-x86_64.exe
# cp target/release/cozo_c.lib release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib
# cp target/release/cozo_c.dll release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll
# cp target/release/cozo_java.dll release/libcozo_java-${COZO_VERSION}-windows-x86_64.dll

# Compress-Archive -Path release/cozoserver-${COZO_VERSION}-windows-x86_64.exe -DestinationPath release/cozoserver-${COZO_VERSION}-windows-x86_64.zip
# Compress-Archive -Path release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib -DestinationPath release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib.zip
# Compress-Archive -Path release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll -DestinationPath release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll.zip
# Compress-Archive -Path release/libcozo_java-${COZO_VERSION}-windows-x86_64.dll -DestinationPath release/libcozo_java-${COZO_VERSION}-windows-x86_64.dll.zip
# Remove-Item release/cozoserver-${COZO_VERSION}-windows-x86_64.exe
# Remove-Item release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib
# Remove-Item release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll
# gzip release/libcozo_java-${COZO_VERSION}-windows-x86_64.dll