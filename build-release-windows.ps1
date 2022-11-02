$ErrorActionPreference="Stop"

$COZO_VERSION = cat Cargo.toml | select-string '([0-9.]+)' | select-string '^version' | ForEach-Object { 
        $_.Matches[0].Groups[1].Value
    }

echo $COZO_VERSION

if (test-path release) {
    Remove-Item -Recurse -Force release
}

mkdir release

cargo build --release
cargo build --release --manifest-path=cozo-lib-c/Cargo.toml

cp target/release/cozoserver.exe release/cozoserver-${COZO_VERSION}-windows-x86_64.exe
cp target/release/cozo_c.dll release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll
cp target/release/cozo_c.lib release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib

Compress-Archive -Path release/cozoserver-${COZO_VERSION}-windows-x86_64.exe -DestinationPath release/cozoserver-${COZO_VERSION}-windows-x86_64.zip
Compress-Archive -Path release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll -DestinationPath release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll.zip
Compress-Archive -Path release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib -DestinationPath release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib.zip
Remove-Item release/cozoserver-${COZO_VERSION}-windows-x86_64.exe
Remove-Item release/libcozo_c-${COZO_VERSION}-windows-x86_64.dll
Remove-Item release/libcozo_c-${COZO_VERSION}-windows-x86_64.lib