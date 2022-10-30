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

cp target/release/cozoserver.exe release/cozoserver-${COZO_VERSION}-windows-x86_64.exe

Compress-Archive -Path release/cozoserver-${COZO_VERSION}-windows-x86_64.exe -DestinationPath release/cozoserver-${COZO_VERSION}-windows-x86_64.zip
Remove-Item release/cozoserver-${COZO_VERSION}-windows-x86_64.exe